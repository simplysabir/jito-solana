//! The `tpu` module implements the Transaction Processing Unit, a
//! multi-stage transaction processing pipeline in software.

pub use solana_sdk::net::DEFAULT_TPU_COALESCE;
use {
    crate::{
        banking_stage::BankingStage,
        banking_trace::{BankingTracer, Channels, TracerThread},
        bundle_stage::{bundle_account_locker::BundleAccountLocker, BundleStage},
        cluster_info_vote_listener::{
            ClusterInfoVoteListener, DuplicateConfirmedSlotsSender, GossipVerifiedVoteHashSender,
            VerifiedVoteSender, VoteTracker,
        },
        fetch_stage::FetchStage,
        proxy::{
            block_engine_stage::{BlockBuilderFeeInfo, BlockEngineConfig, BlockEngineStage},
            fetch_stage_manager::FetchStageManager,
            relayer_stage::{RelayerConfig, RelayerStage},
        },
        sigverify::TransactionSigVerifier,
        sigverify_stage::SigVerifyStage,
        staked_nodes_updater_service::StakedNodesUpdaterService,
        tip_manager::{TipManager, TipManagerConfig},
        tpu_entry_notifier::TpuEntryNotifier,
        validator::{BlockProductionMethod, GeneratorConfig},
    },
    bytes::Bytes,
    crossbeam_channel::{unbounded, Receiver},
    solana_client::connection_cache::ConnectionCache,
    solana_gossip::cluster_info::ClusterInfo,
    solana_ledger::{
        blockstore::Blockstore, blockstore_processor::TransactionStatusSender,
        entry_notifier_service::EntryNotifierSender,
    },
    solana_poh::poh_recorder::{PohRecorder, WorkingBankEntry},
    solana_rpc::{
        optimistically_confirmed_bank_tracker::BankNotificationSender,
        rpc_subscriptions::RpcSubscriptions,
    },
    solana_runtime::{
        bank::Bank,
        bank_forks::BankForks,
        prioritization_fee_cache::PrioritizationFeeCache,
        vote_sender_types::{ReplayVoteReceiver, ReplayVoteSender},
    },
    solana_sdk::{
        clock::Slot,
        pubkey::Pubkey,
        quic::NotifyKeyUpdate,
        signature::{Keypair, Signer},
    },
    solana_streamer::{
        quic::{
            spawn_server_multi, QuicServerParams, SpawnServerResult, MAX_STAKED_CONNECTIONS,
            MAX_UNSTAKED_CONNECTIONS,
        },
        streamer::StakedNodes,
    },
    solana_turbine::broadcast_stage::{BroadcastStage, BroadcastStageType},
    std::{
        collections::{HashMap, HashSet},
        net::{SocketAddr, UdpSocket},
        sync::{atomic::AtomicBool, Arc, Mutex, RwLock},
        thread,
        time::Duration,
    },
    tokio::sync::mpsc::Sender as AsyncSender,
};

// allow multiple connections for NAT and any open/close overlap
pub const MAX_QUIC_CONNECTIONS_PER_PEER: usize = 8;

pub struct TpuSockets {
    pub transactions: Vec<UdpSocket>,
    pub transaction_forwards: Vec<UdpSocket>,
    pub vote: Vec<UdpSocket>,
    pub broadcast: Vec<UdpSocket>,
    pub transactions_quic: Vec<UdpSocket>,
    pub transactions_forwards_quic: Vec<UdpSocket>,
    pub vote_quic: Vec<UdpSocket>,
}

/// For the first `reserved_ticks` ticks of a bank, the preallocated_bundle_cost is subtracted
/// from the Bank's block cost limit.
fn calculate_block_cost_limit_reservation(
    bank: &Bank,
    reserved_ticks: u64,
    preallocated_bundle_cost: u64,
) -> u64 {
    if bank.tick_height() % bank.ticks_per_slot() < reserved_ticks {
        preallocated_bundle_cost
    } else {
        0
    }
}

pub struct Tpu {
    fetch_stage: FetchStage,
    sigverify_stage: SigVerifyStage,
    vote_sigverify_stage: SigVerifyStage,
    banking_stage: BankingStage,
    cluster_info_vote_listener: ClusterInfoVoteListener,
    broadcast_stage: BroadcastStage,
    tpu_quic_t: thread::JoinHandle<()>,
    tpu_forwards_quic_t: thread::JoinHandle<()>,
    tpu_entry_notifier: Option<TpuEntryNotifier>,
    staked_nodes_updater_service: StakedNodesUpdaterService,
    tracer_thread_hdl: TracerThread,
    tpu_vote_quic_t: thread::JoinHandle<()>,
    relayer_stage: RelayerStage,
    block_engine_stage: BlockEngineStage,
    fetch_stage_manager: FetchStageManager,
    bundle_stage: BundleStage,
}

impl Tpu {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cluster_info: &Arc<ClusterInfo>,
        poh_recorder: &Arc<RwLock<PohRecorder>>,
        entry_receiver: Receiver<WorkingBankEntry>,
        retransmit_slots_receiver: Receiver<Slot>,
        sockets: TpuSockets,
        subscriptions: &Arc<RpcSubscriptions>,
        transaction_status_sender: Option<TransactionStatusSender>,
        entry_notification_sender: Option<EntryNotifierSender>,
        blockstore: Arc<Blockstore>,
        broadcast_type: &BroadcastStageType,
        exit: Arc<AtomicBool>,
        shred_version: u16,
        vote_tracker: Arc<VoteTracker>,
        bank_forks: Arc<RwLock<BankForks>>,
        verified_vote_sender: VerifiedVoteSender,
        gossip_verified_vote_hash_sender: GossipVerifiedVoteHashSender,
        replay_vote_receiver: ReplayVoteReceiver,
        replay_vote_sender: ReplayVoteSender,
        bank_notification_sender: Option<BankNotificationSender>,
        tpu_coalesce: Duration,
        duplicate_confirmed_slot_sender: DuplicateConfirmedSlotsSender,
        connection_cache: &Arc<ConnectionCache>,
        turbine_quic_endpoint_sender: AsyncSender<(SocketAddr, Bytes)>,
        keypair: &Keypair,
        log_messages_bytes_limit: Option<usize>,
        staked_nodes: &Arc<RwLock<StakedNodes>>,
        shared_staked_nodes_overrides: Arc<RwLock<HashMap<Pubkey, u64>>>,
        banking_tracer: Arc<BankingTracer>,
        tracer_thread_hdl: TracerThread,
        tpu_enable_udp: bool,
        tpu_max_connections_per_ipaddr_per_minute: u64,
        prioritization_fee_cache: &Arc<PrioritizationFeeCache>,
        block_production_method: BlockProductionMethod,
        enable_block_production_forwarding: bool,
        _generator_config: Option<GeneratorConfig>, /* vestigial code for replay invalidator */
        block_engine_config: Arc<Mutex<BlockEngineConfig>>,
        relayer_config: Arc<Mutex<RelayerConfig>>,
        tip_manager_config: TipManagerConfig,
        shred_receiver_address: Arc<RwLock<Option<SocketAddr>>>,
        preallocated_bundle_cost: u64,
    ) -> (Self, Vec<Arc<dyn NotifyKeyUpdate + Sync + Send>>) {
        let TpuSockets {
            transactions: transactions_sockets,
            transaction_forwards: tpu_forwards_sockets,
            vote: tpu_vote_sockets,
            broadcast: broadcast_sockets,
            transactions_quic: transactions_quic_sockets,
            transactions_forwards_quic: transactions_forwards_quic_sockets,
            vote_quic: tpu_vote_quic_sockets,
        } = sockets;

        // Packets from fetch stage and quic server are intercepted and sent through fetch_stage_manager
        // If relayer is connected, packets are dropped. If not, packets are forwarded on to packet_sender
        let (packet_intercept_sender, packet_intercept_receiver) = unbounded();

        let (vote_packet_sender, vote_packet_receiver) = unbounded();
        let (forwarded_packet_sender, forwarded_packet_receiver) = unbounded();
        let fetch_stage = FetchStage::new_with_sender(
            transactions_sockets,
            tpu_forwards_sockets,
            tpu_vote_sockets,
            exit.clone(),
            &packet_intercept_sender,
            &vote_packet_sender,
            &forwarded_packet_sender,
            forwarded_packet_receiver,
            poh_recorder,
            tpu_coalesce,
            Some(bank_forks.read().unwrap().get_vote_only_mode_signal()),
            tpu_enable_udp,
        );

        let staked_nodes_updater_service = StakedNodesUpdaterService::new(
            exit.clone(),
            bank_forks.clone(),
            staked_nodes.clone(),
            shared_staked_nodes_overrides,
        );

        let Channels {
            non_vote_sender,
            non_vote_receiver,
            tpu_vote_sender,
            tpu_vote_receiver,
            gossip_vote_sender,
            gossip_vote_receiver,
        } = banking_tracer.create_channels(false);

        // Streamer for Votes:
        let SpawnServerResult {
            endpoints: _,
            thread: tpu_vote_quic_t,
            key_updater: vote_streamer_key_updater,
        } = spawn_server_multi(
            "solQuicTVo",
            "quic_streamer_tpu_vote",
            tpu_vote_quic_sockets,
            keypair,
            vote_packet_sender.clone(),
            exit.clone(),
            staked_nodes.clone(),
            QuicServerParams {
                max_connections_per_peer: 1,
                max_connections_per_ipaddr_per_min: tpu_max_connections_per_ipaddr_per_minute,
                coalesce: tpu_coalesce,
                max_staked_connections: MAX_STAKED_CONNECTIONS
                    .saturating_add(MAX_UNSTAKED_CONNECTIONS),
                max_unstaked_connections: 0,
                ..QuicServerParams::default()
            },
        )
        .unwrap();

        // Streamer for TPU
        let SpawnServerResult {
            endpoints: _,
            thread: tpu_quic_t,
            key_updater,
        } = spawn_server_multi(
            "solQuicTpu",
            "quic_streamer_tpu",
            transactions_quic_sockets,
            keypair,
            packet_intercept_sender,
            exit.clone(),
            staked_nodes.clone(),
            QuicServerParams {
                max_connections_per_peer: MAX_QUIC_CONNECTIONS_PER_PEER,
                max_connections_per_ipaddr_per_min: tpu_max_connections_per_ipaddr_per_minute,
                coalesce: tpu_coalesce,
                ..QuicServerParams::default()
            },
        )
        .unwrap();

        // Streamer for TPU forward
        let SpawnServerResult {
            endpoints: _,
            thread: tpu_forwards_quic_t,
            key_updater: forwards_key_updater,
        } = spawn_server_multi(
            "solQuicTpuFwd",
            "quic_streamer_tpu_forwards",
            transactions_forwards_quic_sockets,
            keypair,
            forwarded_packet_sender,
            exit.clone(),
            staked_nodes.clone(),
            QuicServerParams {
                max_connections_per_peer: MAX_QUIC_CONNECTIONS_PER_PEER,
                max_staked_connections: MAX_STAKED_CONNECTIONS
                    .saturating_add(MAX_UNSTAKED_CONNECTIONS),
                max_unstaked_connections: 0, // Prevent unstaked nodes from forwarding transactions
                max_connections_per_ipaddr_per_min: tpu_max_connections_per_ipaddr_per_minute,
                coalesce: tpu_coalesce,
                ..QuicServerParams::default()
            },
        )
        .unwrap();

        let (packet_sender, packet_receiver) = unbounded();

        let sigverify_stage = {
            let verifier = TransactionSigVerifier::new(non_vote_sender.clone());
            SigVerifyStage::new(packet_receiver, verifier, "solSigVerTpu", "tpu-verifier")
        };

        let vote_sigverify_stage = {
            let verifier = TransactionSigVerifier::new_reject_non_vote(tpu_vote_sender);
            SigVerifyStage::new(
                vote_packet_receiver,
                verifier,
                "solSigVerTpuVot",
                "tpu-vote-verifier",
            )
        };

        let block_builder_fee_info = Arc::new(Mutex::new(BlockBuilderFeeInfo {
            block_builder: cluster_info.keypair().pubkey(),
            block_builder_commission: 0,
        }));

        let (bundle_sender, bundle_receiver) = unbounded();
        let block_engine_stage = BlockEngineStage::new(
            block_engine_config,
            bundle_sender,
            cluster_info.clone(),
            packet_sender.clone(),
            non_vote_sender.clone(),
            exit.clone(),
            &block_builder_fee_info,
        );

        let (heartbeat_tx, heartbeat_rx) = unbounded();
        let fetch_stage_manager = FetchStageManager::new(
            cluster_info.clone(),
            heartbeat_rx,
            packet_intercept_receiver,
            packet_sender.clone(),
            exit.clone(),
        );

        let relayer_stage = RelayerStage::new(
            relayer_config,
            cluster_info.clone(),
            heartbeat_tx,
            packet_sender,
            non_vote_sender,
            exit.clone(),
        );

        let cluster_info_vote_listener = ClusterInfoVoteListener::new(
            exit.clone(),
            cluster_info.clone(),
            gossip_vote_sender,
            vote_tracker,
            bank_forks.clone(),
            subscriptions.clone(),
            verified_vote_sender,
            gossip_verified_vote_hash_sender,
            replay_vote_receiver,
            blockstore.clone(),
            bank_notification_sender,
            duplicate_confirmed_slot_sender,
        );

        let tip_manager = TipManager::new(tip_manager_config);

        let bundle_account_locker = BundleAccountLocker::default();

        // The tip program can't be used in BankingStage to avoid someone from stealing tips mid-slot.
        // The first 80% of the block, based on poh ticks, has `preallocated_bundle_cost` less compute units.
        // The last 20% has has full compute so blockspace is maximized if BundleStage is idle.
        let reserved_ticks = poh_recorder
            .read()
            .unwrap()
            .ticks_per_slot()
            .saturating_mul(8)
            .saturating_div(10);

        let mut blacklisted_accounts = HashSet::new();
        blacklisted_accounts.insert(tip_manager.tip_payment_program_id());
        blacklisted_accounts
            .insert(Pubkey::from_str("KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("27G8MtK7VtTcCHkpASjSDdkWWYfoqT6ggEuKidVJidD4").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("4UpD2fh7xH3VP9QQaXtsS1YY3bxzWhtfpks7FatyKvdY").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("LipsxuAkFkwa4RKNzn51wAsW7Dedzt1RNHMkTkDEZUW").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("So1endDq2YkqhipRh3WViPa8hdiSpxWy6z3Z6tMCpAo").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("2Fwvr3MKhHhqakgjjEWcpWZZabbRCetHjukHi1zfKxjk").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("5pHk2TmnqQzRF9L6egy5FfiyBgS7G9cMZ5RFaJAvghzw").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("9RuqAN42PTUi9ya59k9suGATrkqzvb9gk2QABJtQzGP5").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("DdZR6zRFiUt4S5mg7AV1uKB2z1f1WzcNYCaTEEWPAuby").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("7RCz8wb6WXxUhAigok9ttgrVgDFFFbibcirECzWSBauM").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("55YceCDfyvdcPPozDiMeNp9TpwmL1hdoTEFw5BMNWbpf").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("UTABCRXirrbpCNDogCoqEECtM3V44jXGCsK23ZepV3Z").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("EjUgEaPpKMg2nqex9obb46gZQ6Ar9mWSdVKbw9A6PyXA").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("AVxnqyCameKsKTCGVKeyJMA7vjHnxJit6afC8AM9MdMj").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("HKijBKC2zKcV2BXA9CuNemmWUhTuFkPLLgvQBP7zrQjL").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("6LtLpnUFNByNXLyCoK9wA2MykKAmQNZKBdY8s47dehDc").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("7u3HeHxYDLhnCoErrtycNokbQYbWGzLs6JSDqGAv5PfF").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("d4A2prbA2whesmvHaL88BH6Ewn5N4bTSU2Ze8P6Bc4Q").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("HYnVhjsvU1vBKTPsXs1dWe6cJeuU8E4gjoYpmwe81KzN").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("H3t6qZ1JkguCNTi9uzVKqQ7dvt2cum4XiXWom6Gn5e5S").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("2gc9Dm1eB6UgVYFBUN9bWks6Kes9PbWSaPaa9DqyvEiN").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("D6q6wuQSrifJKZYpR1M8R4YawnLDtDsMmWM1NbBmgJ59").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("3NJYftD5sjVfxSnUdZ1wVML8f3aC6mp1CXCL6L7TnU8C").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("i7u4r16TcsJTgq1kAG8opmVZyVnAKBwLKu6ZPMwzxNc").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("24Uqj9JCLxUeoC3hGfh5W3s9FM9uCHDS2SG3LYwBpyTi").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("FL3X2pRsQ9zHENpZSKDRREtccwJuei8yg9fwDu9UN69Q").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("AuQaustGiaqxRvj2gtCdrd22PBzTn8kM3kEPEkZCtuDw").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("4o3qAErcapJ6gRLh1m1x4saoLLieWDu7Rx3wpwLc7Zk9").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("13gDzEXCdocbj8iAiqrScGo47NiSuYENGsRqi3SEAwet").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("4yCLi5yWGzpTWMQ1iWHG5CrGYAdBkhyEdsuSugjDUqwj").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("5GAFPnocJ4GUDJJxtExBDsH5wXzJd3RYzG8goGGCneJi").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("9nD5AenzdbhRqWo7JufdNBbC4VjZ5QH7jzLuvPZy2rhb").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("4LLbsb5ReP3yEtYzmXewyGjcir5uXtKFURtaEUVC2AHs").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("3parcLrT7WnXAcyPfkCz49oofuuf2guUKkjuFkAhZW8Y").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("PaRCLKPpkfHQfXTruT8yhEUx5oRNH8z8erBnzEerc8a").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("Di9ZVJeJrRZdQEWzAFYmfjukjR5dUQb7KMaDmv34rNJg").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("2gWf5xLAzZaKX9tQj9vuXsaxTWtzTZDFRn21J3zjNVgu").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("Ai9AuTfGncuFxEknjZT4HU21Rkv98M1QyXpbW9Xct6LK").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("82dGS7Jt4Km8ZgwZVRsJ2V6vPXEhVdgDaMP7cqPGG1TW").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("D36r7C1FeBUARN7f6mkzdX67UJ1b1nUJKC7SWBpDNWsa").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("5tu3xkmLfud5BAwSuQke4WSjoHcQ52SbrPwX9es8j6Ve").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("4MmJVdwYN8LwvbGeCowYjSx7KoEi6BJWg8XXnW4fDDp6").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("CPNEkz5SaAcWqGMezXTti39ekErzMpDCtuPMGw9tt4CZ").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("s1aysqpEyZyijPybUV89oBGeooXrR22wMNLjnG2SWJA").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("FhVcYNEe58SMtxpZGnTu2kpYJrTu2vwCZDGpPLqbd2yG").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("MangoCzJ36AjZyKwVj3VnYU4GTonjfVEnJmvvWaxLac").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("4MangoMjqJ2firMokCjjGgoK8d4MXcrgL7XJaL3w6fVg").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("FP4PxqHTVzeG2c6eZd7974F9WvKUSdBeduUK3rjYyvBw").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("GqTPL6qRf5aUuqscLh8Rg2HTxPUXfhhAXDptTLhp1t2J").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("FLASH6Lo6h3iasJKWDs2F8TkW2UKf3s15C8PMGuVfgBn").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("81xGAvJ27ZeRThU2JEfKAUeT4Fx6qCCd8WHZpujZbiiG").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("C2S18CZ7hkRV31pSYxANpSrjaZ6mxVJGZrSesL13x2FJ").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("EHBN9YKtMmrZhj8JZqyBQRGqyyeHw5xUB1Q5eAHszuMt").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("NXFiKimQN3QSL3CDhCXddyVmLfrai8HK36bHKaAzK7g").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("xQgT5G6Cf7k6c1YJ7T9e7czdXkmQD1nHH3hdc7w82Wu").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("FWbbZXbfRncNJKy5CnNKykKq4v7qESuykE3KvNodnsFe").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("BAsnXPVYuvZDfEFR7tmu9sG9gPyHy58Jpjs2AuUw1FLx").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("6W9yiHDCW9EpropkFV8R3rPiL8LVWUHSiys3YeW6AT6S").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("Bzjkrm1bFwVXUaV9HTnwxFrPtNso7dnwPQamhqSxtuhZ").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("CU4eFxpyCGNDEXN27Jonn7RfgwBt3cnp7TcTrJF6EW9Q").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("DriFtupJYLTosbwoN8koMbEYSx54aFAVLddWsbksjwg7").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("JCNCMFXo5M5qwUPg2Utu1u6YWp3MbygxqBsBeXXJfrw").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("GXWqPpjQpdz7KZw9p7f5PX2eGxHAhvpNXiviFkAB8zXg").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("Bt2WPMmbwHPk36i4CRucNDyLcmoGdC7xEdrVuxgJaNE6").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("4bcFeLv4nydFrsZqV5CgwCVrPhkQKsXtzfy2KyMz7ozM").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("stkTLPiBsQBUxDhXgxxsTRtxZ38TLqsqhoMvKMSt8Th").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("5JQ8Mhdp2wv3HWcfjq9Ts8kwzCAeBADFBDAgBznzRsE4").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("TuLipcqtGVXP9XR62wM8WWCm6a9vhLs7T1uoWBk6FDs").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("RAtEwzA1rerjeWip6uMuheQtzykxYCrEQRaSFCCrf2D").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("93KT94uivk9egZVPReW27pmUpiBsHhSV11AmUuSExUVU").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("EmLhAPj7J6LTAnomsLfZUKDtb4t2A8e6eofDSfTwMgkY").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("DY3Rw6BZwf6epvWnVo8DSV6kYptEdCh7HbYmFRpdPxuH").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("3CppdkMFxuz7ASS27pB35EDbwgfUhwrarFYuWDBWWwHB").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("Grk7mshVug1TafphUvuYBrzwRqadtmCcf7GGPoPKkgs6").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("BpYbhwDZGpPvcKw3cSh5f9UqRaHfuxgz3avW9g324LUz").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("4nyfJ4JBsRJLij7VGCVUeHwKSLAAku66ptJamoodY29L").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("6opMSfkHgWsvG5KmZo8y2DuShaDHwXfB6VUuTx6W4Age").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("4Ejjk5w7HAWvmXYT57s5uwn8rs7i61nbpcTRQ9ABB11M").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("4xq7VjrJCU2Smk5JcJToik5hiEJ8RCvECReePP8Jg6q8").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("B2YeVM6Kf3SKYLuH2nfucCmZwy8KJcQpd9e9JEuwv9mt").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("8DeQth4AWPXauRfgAEUy9WpHuyKKyYuNNsH76C5v1Hv7").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("FS7TTuJejy7zjkdJXD9BjeLFZ44ipxxr2qmMMUKMZv6y").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("6K8yrdpm2dVaLSLpqoRJKv7SNuP54xmbv5KULcJzKTHc").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("HWe92F97ywdp9TahubeWWWXk5uMHeyYG6AVGLBXAgZp5").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("E3tsfhxsoD4FkWzipVXoRFHQZCH7ADm8iVWCpLCm7VaR").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("FqX68sM8mLVjzixrj3KJ5CCybDet1HD859CNRCNtyWHw").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("ATojCiLv5EoX9GZBkDQZdmhtYzwSJfPquEs9WpVn3yHF").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("3PqNhPLhrZKuRAoej5gStxGKqwp2CByznA5fjc38Dj4C").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("C59QVvteGSt6nkgRiCbmB22HrM5w3GKvivKC5LvTa5ac").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("5QQ6Eu8i7D4NYSEs1SitZXVqoB6hpTMmaWZWsSW7Wiwb").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("Ayq7bKZ1FWKhXubUq98hQfqUYcHEbEVYzn6H5cB18G2Z").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("HajXYaDXmohtq2ZxZ6QVNEpqNn1T53Zc9FnR1CnaNnUf").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("PmmPGJnGKLRTaGpDXVEXhfgDDkc4DJbApA1eKUWJPMM").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("CJM5Un8AhMgLJv2mcj3o5z2z8H3deDzLA1TH7E3WhZQG").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("9Zmn9v5A2YWUQj47bkEmcnc37ZsYe83rsRK8VV2j1UqX").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("4KvPuh1wG8j1pLnZUC5CuqTm2a41PWNtik1NwpLoRquE").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("Hcs63usAc6cxWccycrVwx1mrNgNSpUZaUgFm7Lw9tSkR").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("ARLwHJ3CYLkVTeW3nHvPBmGQ7SLQdhZbAkWHzYrq57rt").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("FyH3qGRQSG7AmdEsPEVDxdJJLnLhAn3CZ48acQU34LFr").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("MzEPFp2LwCSMMPHLQsqfE7SN6xkPHZ8Uym2HfrH7g5P").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("CMiyE7M98DSPBEhQGTA6CzNodWkNuuW4y9HoocfK75nG").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("CnaXXuzc2S5UFSGoBRuKVNnzXBvxbaMwq6hZu5m91CAV").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("3Nkctq19AW7gs5hkxixUDjS9UVjmCwcNCo7rqPpub87c").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("41Y8C4oxk4zgJT1KXyQr35UhZcfsp5mP86Z2G7UUzojU").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("EuSLjg23BrtwYAk1t4TFe5ArYSXCVXLBqrHRBfWQiTeJ").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("BVddkVtFJLCihbVrtLo8e3iEd9NftuLunaznAxFFW8vf").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("ENr5e1BMN5vFUHf4iCCPzR4GjWCKgtHnQcdniRQqMdEL").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("2r81MPMDjGSrbmGRwzDg6aqhe3t3vbKcrYfpes5bXckS").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("85XuR4kE5yxp1hk91WHAawinXZsuJowxy59STYYpM9pK").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("CxL8eQmGhN9LKSoHj7bU95JekFPtyZoUc57mbehb5A56").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("4cvgasNfbJ36yeMVJSkscgL2Yco9dFGdj52Wrg91fmHv").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("7ngzeBygEksaBvKzHEeihqoLpDpWqTNRMVh2wCyb6NP8").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("CZU38L2NyL6tqFxzYAGYkmkf2JG98tZfZ2CnUapVgXQe").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("DUW6uWcrsjYmsYDjp9iGDN4JdRa2MqznjuxjKVok5Fsj").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("Fd3k4c6Dv7m9673ae87P6duQrftY9UVfwiCxngNbJrUQ").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("9BMEyctGvajEubk5iCRBnM9fkeTXUhrxaweYq34jZdC8").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("6DFDj66PbPoTC16Sh51MJijoTTMYCbMCVC85tnc5UfQ3").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("HTLvAjqc6Wkzh4i4QNLHhQHZAnrtVvkGyYeyCiUWLe9b").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("CYUyHzu6Z3JyBhfkQpZZwWqa2zpcmzaK1xXS96n8ea1U").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("ZETAxsqBRek56DhiGXrn75yj2NHU3aYUnxvHXpkf3aD").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("DdGHYzBoTGJJZ4Npy1AHVuyZsfX89ShauiukqMt8sPRw").unwrap());
        blacklisted_accounts.insert(
            Pubkey::from_str("A1KLoBrKBde8Ty9qtNQUtq3C2ortoC3u7twggz7blacklisted_accountso6")
                .unwrap(),
        );
        blacklisted_accounts
            .insert(Pubkey::from_str("KMNo3nJsBXfcpJTVhZcXLW7RmTwTt4GVFE7suUBo9sS").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("DriFtupJYLTosbwoN8koMbEYSx54aFAVLddWsbksjwg7").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("SLNDpmoWTVADgEdndyvWzroNL7zSi1dF9PC3xHGtPwp").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("4LLbsb5ReP3yEtYzmXewyGjcir5uXtKFURtaEUVC2AHs").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("MangoCzJ36AjZyKwVj3VnYU4GTonjfVEnJmvvWaxLac").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("PRT88RkA4Kg5z7pKnezeNH4mafTvtQdfFgpQTGRjz44").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("TuLipcqtGVXP9XR62wM8WWCm6a9vhLs7T1uoWBk6FDs").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("HBB111SCo9jkCejsZfz8Ec8nH7T6THF8KEKSnvwT6XK6").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("8twuNzMszqWeFbDErwtf4gw13E6MUS4Hsdx5mi3aqXAM").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("SUNNYWgPQmFxe9wTZzNK7iPnJ3vYDrkgnxJRJm1s3ag").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("Lrxqnh6ZHKbGy3dcrCED43nsoLkM1LTzU2jRfWe8qUC").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("DUALa4FC2yREwZ59PHeu1un4wis36vHRv5hWVBmzykCJ").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("PoRTjZMPXb9T7dyU7tpLEZRQj7e6ssfAE62j2oQuc6y").unwrap());
        blacklisted_accounts
            .insert(Pubkey::from_str("APTtJyaRX5yGTsJU522N4VYWg3vCvSb65eam5GrPT5Rt").unwrap());
        let banking_stage = BankingStage::new(
            block_production_method,
            cluster_info,
            poh_recorder,
            non_vote_receiver,
            tpu_vote_receiver,
            gossip_vote_receiver,
            transaction_status_sender.clone(),
            replay_vote_sender.clone(),
            log_messages_bytes_limit,
            connection_cache.clone(),
            bank_forks.clone(),
            prioritization_fee_cache,
            enable_block_production_forwarding,
            blacklisted_accounts,
            bundle_account_locker.clone(),
            move |bank| {
                calculate_block_cost_limit_reservation(
                    bank,
                    reserved_ticks,
                    preallocated_bundle_cost,
                )
            },
        );

        let bundle_stage = BundleStage::new(
            cluster_info,
            poh_recorder,
            bundle_receiver,
            transaction_status_sender,
            replay_vote_sender,
            log_messages_bytes_limit,
            exit.clone(),
            tip_manager,
            bundle_account_locker,
            &block_builder_fee_info,
            prioritization_fee_cache,
        );

        let (entry_receiver, tpu_entry_notifier) =
            if let Some(entry_notification_sender) = entry_notification_sender {
                let (broadcast_entry_sender, broadcast_entry_receiver) = unbounded();
                let tpu_entry_notifier = TpuEntryNotifier::new(
                    entry_receiver,
                    entry_notification_sender,
                    broadcast_entry_sender,
                    exit.clone(),
                );
                (broadcast_entry_receiver, Some(tpu_entry_notifier))
            } else {
                (entry_receiver, None)
            };

        let broadcast_stage = broadcast_type.new_broadcast_stage(
            broadcast_sockets,
            cluster_info.clone(),
            entry_receiver,
            retransmit_slots_receiver,
            exit,
            blockstore,
            bank_forks,
            shred_version,
            turbine_quic_endpoint_sender,
            shred_receiver_address,
        );

        (
            Self {
                fetch_stage,
                sigverify_stage,
                vote_sigverify_stage,
                banking_stage,
                cluster_info_vote_listener,
                broadcast_stage,
                tpu_quic_t,
                tpu_forwards_quic_t,
                tpu_entry_notifier,
                staked_nodes_updater_service,
                tracer_thread_hdl,
                tpu_vote_quic_t,
                block_engine_stage,
                relayer_stage,
                fetch_stage_manager,
                bundle_stage,
            },
            vec![key_updater, forwards_key_updater, vote_streamer_key_updater],
        )
    }

    pub fn join(self) -> thread::Result<()> {
        let results = vec![
            self.fetch_stage.join(),
            self.sigverify_stage.join(),
            self.vote_sigverify_stage.join(),
            self.cluster_info_vote_listener.join(),
            self.banking_stage.join(),
            self.staked_nodes_updater_service.join(),
            self.tpu_quic_t.join(),
            self.tpu_forwards_quic_t.join(),
            self.tpu_vote_quic_t.join(),
            self.bundle_stage.join(),
            self.relayer_stage.join(),
            self.block_engine_stage.join(),
            self.fetch_stage_manager.join(),
        ];
        let broadcast_result = self.broadcast_stage.join();
        for result in results {
            result?;
        }
        if let Some(tpu_entry_notifier) = self.tpu_entry_notifier {
            tpu_entry_notifier.join()?;
        }
        let _ = broadcast_result?;
        if let Some(tracer_thread_hdl) = self.tracer_thread_hdl {
            if let Err(tracer_result) = tracer_thread_hdl.join()? {
                error!(
                    "banking tracer thread returned error after successful thread join: {:?}",
                    tracer_result
                );
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use {
        super::calculate_block_cost_limit_reservation,
        solana_ledger::genesis_utils::create_genesis_config, solana_pubkey::Pubkey,
        solana_runtime::bank::Bank, std::sync::Arc,
    };

    #[test]
    fn test_calculate_block_cost_limit_reservation() {
        const BUNDLE_BLOCK_COST_LIMITS_RESERVATION: u64 = 100;
        const RESERVED_TICKS: u64 = 5;
        let genesis_config_info = create_genesis_config(100);
        let bank = Arc::new(Bank::new_for_tests(&genesis_config_info.genesis_config));

        for _ in 0..genesis_config_info.genesis_config.ticks_per_slot {
            bank.register_default_tick_for_test();
        }
        assert!(bank.is_complete());
        bank.freeze();
        let bank1 = Arc::new(Bank::new_from_parent(bank.clone(), &Pubkey::default(), 1));

        // wait for reservation to be over
        (0..RESERVED_TICKS).for_each(|_| {
            assert_eq!(
                calculate_block_cost_limit_reservation(
                    &bank1,
                    RESERVED_TICKS,
                    BUNDLE_BLOCK_COST_LIMITS_RESERVATION,
                ),
                BUNDLE_BLOCK_COST_LIMITS_RESERVATION
            );
            bank1.register_default_tick_for_test();
        });
        assert_eq!(
            calculate_block_cost_limit_reservation(
                &bank1,
                RESERVED_TICKS,
                BUNDLE_BLOCK_COST_LIMITS_RESERVATION,
            ),
            0
        );
    }
}
