/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.Version;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.Nullable;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedIndexMetadata;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.node.Node;
import org.opensearch.node.remotestore.RemoteStoreNodeAttribute;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.repositories.blobstore.ChecksumBlobStoreFormat;
import org.opensearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.opensearch.gateway.PersistedClusterStateService.SLOW_WRITE_LOGGING_THRESHOLD;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.isRemoteStoreClusterStateEnabled;

/**
 * A Service which provides APIs to upload and download cluster metadata from remote store.
 *
 * @opensearch.internal
 */
public class RemoteClusterStateService implements Closeable {

    public static final String METADATA_NAME_FORMAT = "%s.dat";

    public static final String METADATA_MANIFEST_NAME_FORMAT = "%s";

    public static final int RETAINED_MANIFESTS = 10;

    public static final String DELIMITER = "__";

    private static final Logger logger = LogManager.getLogger(RemoteClusterStateService.class);

    public static final int INDEX_METADATA_UPLOAD_WAIT_MILLIS = 20000;

    public static final ChecksumBlobStoreFormat<IndexMetadata> INDEX_METADATA_FORMAT = new ChecksumBlobStoreFormat<>(
        "index-metadata",
        METADATA_NAME_FORMAT,
        IndexMetadata::fromXContent
    );

    public static final ChecksumBlobStoreFormat<ClusterMetadataManifest> CLUSTER_METADATA_MANIFEST_FORMAT = new ChecksumBlobStoreFormat<>(
        "cluster-metadata-manifest",
        METADATA_MANIFEST_NAME_FORMAT,
        ClusterMetadataManifest::fromXContent
    );
    /**
     * Used to specify if cluster state metadata should be published to remote store
     */
    public static final Setting<Boolean> REMOTE_CLUSTER_STATE_ENABLED_SETTING = Setting.boolSetting(
        "cluster.remote_store.state.enabled",
        false,
        Property.NodeScope,
        Property.Final
    );

    private static final String CLUSTER_STATE_PATH_TOKEN = "cluster-state";
    private static final String INDEX_PATH_TOKEN = "index";
    private static final String MANIFEST_PATH_TOKEN = "manifest";
    private static final String MANIFEST_FILE_PREFIX = "manifest";
    private static final String INDEX_METADATA_FILE_PREFIX = "metadata";

    private final String nodeId;
    private final Supplier<RepositoriesService> repositoriesService;
    private final Settings settings;
    private final LongSupplier relativeTimeNanosSupplier;
    private final ThreadPool threadpool;
    private BlobStoreRepository blobStoreRepository;
    private BlobStoreTransferService blobStoreTransferService;
    private volatile TimeValue slowWriteLoggingThreshold;

    private final AtomicBoolean deleteStaleMetadataRunning = new AtomicBoolean(false);

    public RemoteClusterStateService(
        String nodeId,
        Supplier<RepositoriesService> repositoriesService,
        Settings settings,
        ClusterSettings clusterSettings,
        LongSupplier relativeTimeNanosSupplier,
        ThreadPool threadPool
    ) {
        assert isRemoteStoreClusterStateEnabled(settings) : "Remote cluster state is not enabled";
        this.nodeId = nodeId;
        this.repositoriesService = repositoriesService;
        this.settings = settings;
        this.relativeTimeNanosSupplier = relativeTimeNanosSupplier;
        this.threadpool = threadPool;
        this.slowWriteLoggingThreshold = clusterSettings.get(SLOW_WRITE_LOGGING_THRESHOLD);
        clusterSettings.addSettingsUpdateConsumer(SLOW_WRITE_LOGGING_THRESHOLD, this::setSlowWriteLoggingThreshold);
    }

    private BlobStoreTransferService getBlobStoreTransferService() {
        if (blobStoreTransferService == null) {
            blobStoreTransferService = new BlobStoreTransferService(blobStoreRepository.blobStore(), threadpool);
        }
        return blobStoreTransferService;
    }

    /**
     * This method uploads entire cluster state metadata to the configured blob store. For now only index metadata upload is supported. This method should be
     * invoked by the elected cluster manager when the remote cluster state is enabled.
     *
     * @return A manifest object which contains the details of uploaded entity metadata.
     */
    @Nullable
    public ClusterMetadataManifest writeFullMetadata(ClusterState clusterState) throws IOException {
        final long startTimeNanos = relativeTimeNanosSupplier.getAsLong();
        if (clusterState.nodes().isLocalNodeElectedClusterManager() == false) {
            logger.error("Local node is not elected cluster manager. Exiting");
            return null;
        }

        // should fetch the previous cluster UUID before writing full cluster state.
        // Whenever a new election happens, a new leader will be elected and it might have stale previous UUID
        final String previousClusterUUID = fetchPreviousClusterUUID(
            clusterState.getClusterName().value(),
            clusterState.metadata().clusterUUID()
        );

        // any validations before/after upload ?
        final List<UploadedIndexMetadata> allUploadedIndexMetadata = writeIndexMetadataParallel(
            clusterState,
            new ArrayList<>(clusterState.metadata().indices().values())
        );
        final ClusterMetadataManifest manifest = uploadManifest(clusterState, allUploadedIndexMetadata, previousClusterUUID, false);
        final long durationMillis = TimeValue.nsecToMSec(relativeTimeNanosSupplier.getAsLong() - startTimeNanos);
        if (durationMillis >= slowWriteLoggingThreshold.getMillis()) {
            logger.warn(
                "writing cluster state took [{}ms] which is above the warn threshold of [{}]; " + "wrote full state with [{}] indices",
                durationMillis,
                slowWriteLoggingThreshold,
                allUploadedIndexMetadata.size()
            );
        } else {
            // todo change to debug
            logger.info(
                "writing cluster state took [{}ms]; " + "wrote full state with [{}] indices",
                durationMillis,
                allUploadedIndexMetadata.size()
            );
        }
        return manifest;
    }

    /**
     * This method uploads the diff between the previous cluster state and the current cluster state. The previous manifest file is needed to create the new
     * manifest. The new manifest file is created by using the unchanged metadata from the previous manifest and the new metadata changes from the current
     * cluster state.
     *
     * @return The uploaded ClusterMetadataManifest file
     */
    @Nullable
    public ClusterMetadataManifest writeIncrementalMetadata(
        ClusterState previousClusterState,
        ClusterState clusterState,
        ClusterMetadataManifest previousManifest
    ) throws IOException {
        final long startTimeNanos = relativeTimeNanosSupplier.getAsLong();
        if (clusterState.nodes().isLocalNodeElectedClusterManager() == false) {
            logger.error("Local node is not elected cluster manager. Exiting");
            return null;
        }
        assert previousClusterState.metadata().coordinationMetadata().term() == clusterState.metadata().coordinationMetadata().term();
        final Map<String, Long> previousStateIndexMetadataVersionByName = new HashMap<>();
        for (final IndexMetadata indexMetadata : previousClusterState.metadata().indices().values()) {
            previousStateIndexMetadataVersionByName.put(indexMetadata.getIndex().getName(), indexMetadata.getVersion());
        }

        int numIndicesUpdated = 0;
        int numIndicesUnchanged = 0;
        final Map<String, ClusterMetadataManifest.UploadedIndexMetadata> allUploadedIndexMetadata = previousManifest.getIndices()
            .stream()
            .collect(Collectors.toMap(UploadedIndexMetadata::getIndexName, Function.identity()));

        List<IndexMetadata> toUpload = new ArrayList<>();

        for (final IndexMetadata indexMetadata : clusterState.metadata().indices().values()) {
            final Long previousVersion = previousStateIndexMetadataVersionByName.get(indexMetadata.getIndex().getName());
            if (previousVersion == null || indexMetadata.getVersion() != previousVersion) {
                logger.trace(
                    "updating metadata for [{}], changing version from [{}] to [{}]",
                    indexMetadata.getIndex(),
                    previousVersion,
                    indexMetadata.getVersion()
                );
                numIndicesUpdated++;
                toUpload.add(indexMetadata);
            } else {
                numIndicesUnchanged++;
            }
            previousStateIndexMetadataVersionByName.remove(indexMetadata.getIndex().getName());
        }

        List<UploadedIndexMetadata> uploadedIndexMetadataList = writeIndexMetadataParallel(clusterState, toUpload);
        uploadedIndexMetadataList.forEach(
            uploadedIndexMetadata -> allUploadedIndexMetadata.put(uploadedIndexMetadata.getIndexName(), uploadedIndexMetadata)
        );

        for (String removedIndexName : previousStateIndexMetadataVersionByName.keySet()) {
            allUploadedIndexMetadata.remove(removedIndexName);
        }
        final ClusterMetadataManifest manifest = uploadManifest(
            clusterState,
            new ArrayList<>(allUploadedIndexMetadata.values()),
            previousManifest.getPreviousClusterUUID(),
            false
        );
        deleteStaleClusterMetadata(clusterState.getClusterName().value(), clusterState.metadata().clusterUUID(), RETAINED_MANIFESTS);

        final long durationMillis = TimeValue.nsecToMSec(relativeTimeNanosSupplier.getAsLong() - startTimeNanos);
        if (durationMillis >= slowWriteLoggingThreshold.getMillis()) {
            logger.warn(
                "writing cluster state took [{}ms] which is above the warn threshold of [{}]; "
                    + "wrote  metadata for [{}] indices and skipped [{}] unchanged indices",
                durationMillis,
                slowWriteLoggingThreshold,
                numIndicesUpdated,
                numIndicesUnchanged
            );
        } else {
            logger.trace(
                "writing cluster state took [{}ms]; " + "wrote metadata for [{}] indices and skipped [{}] unchanged indices",
                durationMillis,
                numIndicesUpdated,
                numIndicesUnchanged
            );
        }
        return manifest;
    }

    /**
     * Uploads provided IndexMetadata's to remote store in parallel. The call is blocking so the method waits for upload to finish and then return.
     *
     * @param clusterState current ClusterState
     * @param toUpload list of IndexMetadata to upload
     * @return {@code List<UploadedIndexMetadata>} list of IndexMetadata uploaded to remote
     */
    private List<UploadedIndexMetadata> writeIndexMetadataParallel(ClusterState clusterState, List<IndexMetadata> toUpload)
        throws IOException {
        List<Exception> exceptionList = Collections.synchronizedList(new ArrayList<>(toUpload.size()));
        final CountDownLatch latch = new CountDownLatch(toUpload.size());
        List<UploadedIndexMetadata> result = new ArrayList<>(toUpload.size());

        LatchedActionListener<UploadedIndexMetadata> latchedActionListener = new LatchedActionListener<>(
            ActionListener.wrap((UploadedIndexMetadata uploadedIndexMetadata) -> {
                logger.trace(
                    String.format(Locale.ROOT, "IndexMetadata uploaded successfully for %s", uploadedIndexMetadata.getIndexName())
                );
                result.add(uploadedIndexMetadata);
            }, ex -> {
                assert ex instanceof IndexMetadataTransferException;
                logger.error(
                    () -> new ParameterizedMessage("Exception during transfer of IndexMetadata to Remote {}", ex.getMessage()),
                    ex
                );
                exceptionList.add(ex);
            }),
            latch
        );

        for (IndexMetadata indexMetadata : toUpload) {
            // 123456789012_test-cluster/cluster-state/dsgYj10Nkso7/index/ftqsCnn9TgOX/metadata_4_1690947200
            writeIndexMetadataAsync(clusterState, indexMetadata, latchedActionListener);
        }

        try {
            if (latch.await(INDEX_METADATA_UPLOAD_WAIT_MILLIS, TimeUnit.MILLISECONDS) == false) {
                IndexMetadataTransferException ex = new IndexMetadataTransferException(
                    String.format(
                        Locale.ROOT,
                        "Timed out waiting for transfer of index metadata to complete - %s",
                        toUpload.stream().map(IndexMetadata::getIndex).map(Index::toString).collect(Collectors.joining(""))
                    )
                );
                exceptionList.forEach(ex::addSuppressed);
                throw ex;
            }
        } catch (InterruptedException ex) {
            exceptionList.forEach(ex::addSuppressed);
            IndexMetadataTransferException exception = new IndexMetadataTransferException(
                String.format(
                    Locale.ROOT,
                    "Timed out waiting for transfer of index metadata to complete - %s",
                    toUpload.stream().map(IndexMetadata::getIndex).map(Index::toString).collect(Collectors.joining(""))
                ),
                ex
            );
            Thread.currentThread().interrupt();
            throw exception;
        }
        if (exceptionList.size() > 0) {
            IndexMetadataTransferException exception = new IndexMetadataTransferException(
                String.format(
                    Locale.ROOT,
                    "Exception during transfer of IndexMetadata to Remote %s",
                    toUpload.stream().map(IndexMetadata::getIndex).map(Index::toString).collect(Collectors.joining(""))
                )
            );
            exceptionList.forEach(exception::addSuppressed);
            throw exception;
        }
        return result;
    }

    /**
     * Allows async Upload of IndexMetadata to remote
     *
     * @param clusterState current ClusterState
     * @param indexMetadata {@link IndexMetadata} to upload
     * @param latchedActionListener listener to respond back on after upload finishes
     */
    private void writeIndexMetadataAsync(
        ClusterState clusterState,
        IndexMetadata indexMetadata,
        LatchedActionListener<UploadedIndexMetadata> latchedActionListener
    ) throws IOException {
        final BlobContainer indexMetadataContainer = indexMetadataContainer(
            clusterState.getClusterName().value(),
            clusterState.metadata().clusterUUID(),
            indexMetadata.getIndexUUID()
        );
        final String indexMetadataFilename = indexMetadataFileName(indexMetadata);
        ActionListener<Void> completionListener = ActionListener.wrap(
            resp -> latchedActionListener.onResponse(
                new UploadedIndexMetadata(
                    indexMetadata.getIndex().getName(),
                    indexMetadata.getIndexUUID(),
                    indexMetadataContainer.path().buildAsString() + indexMetadataFilename
                )
            ),
            ex -> latchedActionListener.onFailure(new IndexMetadataTransferException(indexMetadata.getIndex().toString(), ex))
        );

        INDEX_METADATA_FORMAT.writeAsync(
            indexMetadata,
            indexMetadataContainer,
            indexMetadataFilename,
            blobStoreRepository.getCompressor(),
            completionListener
        );
    }

    @Nullable
    public ClusterMetadataManifest markLastStateAsCommitted(ClusterState clusterState, ClusterMetadataManifest previousManifest)
        throws IOException {
        if (clusterState.nodes().isLocalNodeElectedClusterManager() == false) {
            logger.error("Local node is not elected cluster manager. Exiting");
            return null;
        }
        assert clusterState != null : "Last accepted cluster state is not set";
        assert previousManifest != null : "Last cluster metadata manifest is not set";
        return uploadManifest(clusterState, previousManifest.getIndices(), previousManifest.getPreviousClusterUUID(), true);
    }

    @Override
    public void close() throws IOException {
        if (blobStoreRepository != null) {
            IOUtils.close(blobStoreRepository);
        }
    }

    public void start() {
        assert isRemoteStoreClusterStateEnabled(settings) == true : "Remote cluster state is not enabled";
        final String remoteStoreRepo = settings.get(
            Node.NODE_ATTRIBUTES.getKey() + RemoteStoreNodeAttribute.REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY
        );
        assert remoteStoreRepo != null : "Remote Cluster State repository is not configured";
        final Repository repository = repositoriesService.get().repository(remoteStoreRepo);
        assert repository instanceof BlobStoreRepository : "Repository should be instance of BlobStoreRepository";
        blobStoreRepository = (BlobStoreRepository) repository;
    }

    private ClusterMetadataManifest uploadManifest(
        ClusterState clusterState,
        List<UploadedIndexMetadata> uploadedIndexMetadata,
        String previousClusterUUID,
        boolean committed
    ) throws IOException {
        synchronized (this) {
            final String manifestFileName = getManifestFileName(clusterState.term(), clusterState.version());
            final ClusterMetadataManifest manifest = new ClusterMetadataManifest(
                clusterState.term(),
                clusterState.getVersion(),
                clusterState.metadata().clusterUUID(),
                clusterState.stateUUID(),
                Version.CURRENT,
                nodeId,
                committed,
                uploadedIndexMetadata,
                previousClusterUUID
            );
            writeMetadataManifest(clusterState.getClusterName().value(), clusterState.metadata().clusterUUID(), manifest, manifestFileName);
            return manifest;
        }
    }

    private void writeMetadataManifest(String clusterName, String clusterUUID, ClusterMetadataManifest uploadManifest, String fileName)
        throws IOException {
        final BlobContainer metadataManifestContainer = manifestContainer(clusterName, clusterUUID);
        CLUSTER_METADATA_MANIFEST_FORMAT.write(uploadManifest, metadataManifestContainer, fileName, blobStoreRepository.getCompressor());
    }

    private String fetchPreviousClusterUUID(String clusterName, String clusterUUID) {
        final Optional<ClusterMetadataManifest> latestManifest = getLatestClusterMetadataManifest(clusterName, clusterUUID);
        if (!latestManifest.isPresent()) {
            final String previousClusterUUID = getLastKnownUUIDFromRemote(clusterName);
            assert !clusterUUID.equals(previousClusterUUID) : "Last cluster UUID is same current cluster UUID";
            return previousClusterUUID;
        }
        return latestManifest.get().getPreviousClusterUUID();
    }

    private BlobContainer indexMetadataContainer(String clusterName, String clusterUUID, String indexUUID) {
        // 123456789012_test-cluster/cluster-state/dsgYj10Nkso7/index/ftqsCnn9TgOX
        return blobStoreRepository.blobStore()
            .blobContainer(getCusterMetadataBasePath(clusterName, clusterUUID).add(INDEX_PATH_TOKEN).add(indexUUID));
    }

    private BlobContainer manifestContainer(String clusterName, String clusterUUID) {
        // 123456789012_test-cluster/cluster-state/dsgYj10Nkso7/manifest
        return blobStoreRepository.blobStore().blobContainer(getManifestFolderPath(clusterName, clusterUUID));
    }

    private BlobPath getCusterMetadataBasePath(String clusterName, String clusterUUID) {
        return blobStoreRepository.basePath().add(encodeString(clusterName)).add(CLUSTER_STATE_PATH_TOKEN).add(clusterUUID);
    }

    private BlobContainer clusterUUIDContainer(String clusterName) {
        return blobStoreRepository.blobStore()
            .blobContainer(
                blobStoreRepository.basePath()
                    .add(Base64.getUrlEncoder().withoutPadding().encodeToString(clusterName.getBytes(StandardCharsets.UTF_8)))
                    .add(CLUSTER_STATE_PATH_TOKEN)
            );
    }

    private void setSlowWriteLoggingThreshold(TimeValue slowWriteLoggingThreshold) {
        this.slowWriteLoggingThreshold = slowWriteLoggingThreshold;
    }

    private static String getManifestFileName(long term, long version) {
        // 123456789012_test-cluster/cluster-state/dsgYj10Nkso7/manifest/manifest_2147483642_2147483637_456536447
        return String.join(DELIMITER, getManifestFileNamePrefix(term, version), RemoteStoreUtils.invertLong(System.currentTimeMillis()));
    }

    private static String getManifestFileNamePrefix(long term, long version) {
        // 123456789012_test-cluster/cluster-state/dsgYj10Nkso7/manifest/manifest_2147483642_2147483637
        return String.join(DELIMITER, MANIFEST_PATH_TOKEN, RemoteStoreUtils.invertLong(term), RemoteStoreUtils.invertLong(version));
    }

    private static String indexMetadataFileName(IndexMetadata indexMetadata) {
        return String.join(
            DELIMITER,
            INDEX_METADATA_FILE_PREFIX,
            String.valueOf(indexMetadata.getVersion()),
            String.valueOf(System.currentTimeMillis())
        );
    }

    private BlobPath getManifestFolderPath(String clusterName, String clusterUUID) {
        return getCusterMetadataBasePath(clusterName, clusterUUID).add(MANIFEST_PATH_TOKEN);
    }

    /**
     * Fetch latest index metadata from remote cluster state
     *
     * @param clusterUUID uuid of cluster state to refer to in remote
     * @param clusterName name of the cluster
     * @return {@code Map<String, IndexMetadata>} latest IndexUUID to IndexMetadata map
     */
    public Map<String, IndexMetadata> getLatestIndexMetadata(String clusterName, String clusterUUID) throws IOException {
        start();
        Map<String, IndexMetadata> remoteIndexMetadata = new HashMap<>();
        Optional<ClusterMetadataManifest> clusterMetadataManifest = getLatestClusterMetadataManifest(clusterName, clusterUUID);
        if (!clusterMetadataManifest.isPresent()) {
            throw new IllegalStateException("Latest index metadata is not present for the provided clusterUUID");
        }
        assert Objects.equals(clusterUUID, clusterMetadataManifest.get().getClusterUUID())
            : "Corrupt ClusterMetadataManifest found. Cluster UUID mismatch.";
        for (UploadedIndexMetadata uploadedIndexMetadata : clusterMetadataManifest.get().getIndices()) {
            IndexMetadata indexMetadata = getIndexMetadata(clusterName, clusterUUID, uploadedIndexMetadata);
            remoteIndexMetadata.put(uploadedIndexMetadata.getIndexUUID(), indexMetadata);
        }
        return remoteIndexMetadata;
    }

    /**
     * Fetch index metadata from remote cluster state
     *
     * @param clusterUUID uuid of cluster state to refer to in remote
     * @param clusterName name of the cluster
     * @param uploadedIndexMetadata {@link UploadedIndexMetadata} contains details about remote location of index metadata
     * @return {@link IndexMetadata}
     */
    private IndexMetadata getIndexMetadata(String clusterName, String clusterUUID, UploadedIndexMetadata uploadedIndexMetadata) {
        try {
            String[] splitPath = uploadedIndexMetadata.getUploadedFilename().split("/");
            return INDEX_METADATA_FORMAT.read(
                indexMetadataContainer(clusterName, clusterUUID, uploadedIndexMetadata.getIndexUUID()),
                splitPath[splitPath.length - 1],
                blobStoreRepository.getNamedXContentRegistry()
            );
        } catch (IOException e) {
            throw new IllegalStateException(
                String.format(Locale.ROOT, "Error while downloading IndexMetadata - %s", uploadedIndexMetadata.getUploadedFilename()),
                e
            );
        }
    }

    /**
     * Fetch latest ClusterMetadataManifest from remote state store
     *
     * @param clusterUUID uuid of cluster state to refer to in remote
     * @param clusterName name of the cluster
     * @return ClusterMetadataManifest
     */
    public Optional<ClusterMetadataManifest> getLatestClusterMetadataManifest(String clusterName, String clusterUUID) {
        Optional<String> latestManifestFileName = getLatestManifestFileName(clusterName, clusterUUID);
        if (latestManifestFileName.isPresent()) {
            return Optional.of(fetchRemoteClusterMetadataManifest(clusterName, clusterUUID, latestManifestFileName.get()));
        }
        return Optional.empty();
    }

    /**
     * Fetch the previous cluster UUIDs from remote state store and return the most recent valid cluster UUID
     *
     * @param clusterName The cluster name for which previous cluster UUID is to be fetched
     * @return Last valid cluster UUID
     */
    public String getLastKnownUUIDFromRemote(String clusterName) {
        try {
            Set<String> clusterUUIDs = getAllClusterUUIDs(clusterName);
            Map<String, ClusterMetadataManifest> latestManifests = getLatestManifestForAllClusterUUIDs(clusterName, clusterUUIDs);
            List<String> validChain = createClusterChain(latestManifests);
            if (validChain.isEmpty()) {
                return ClusterState.UNKNOWN_UUID;
            }
            return validChain.get(0);
        } catch (IOException e) {
            throw new IllegalStateException(
                String.format(Locale.ROOT, "Error while fetching previous UUIDs from remote store for cluster name: %s", clusterName)
            );
        }
    }

    private Set<String> getAllClusterUUIDs(String clusterName) throws IOException {
        Map<String, BlobContainer> clusterUUIDMetadata = clusterUUIDContainer(clusterName).children();
        if (clusterUUIDMetadata == null) {
            return Collections.emptySet();
        }
        return Collections.unmodifiableSet(clusterUUIDMetadata.keySet());
    }

    private Map<String, ClusterMetadataManifest> getLatestManifestForAllClusterUUIDs(String clusterName, Set<String> clusterUUIDs) {
        Map<String, ClusterMetadataManifest> manifestsByClusterUUID = new HashMap<>();
        for (String clusterUUID : clusterUUIDs) {
            try {
                Optional<ClusterMetadataManifest> manifest = getLatestClusterMetadataManifest(clusterName, clusterUUID);
                manifest.ifPresent(clusterMetadataManifest -> manifestsByClusterUUID.put(clusterUUID, clusterMetadataManifest));
            } catch (Exception e) {
                throw new IllegalStateException(
                    String.format(Locale.ROOT, "Exception in fetching manifest for clusterUUID: %s", clusterUUID)
                );
            }
        }
        return manifestsByClusterUUID;
    }

    /**
     * This method creates a valid cluster UUID chain.
     *
     * @param manifestsByClusterUUID Map of latest ClusterMetadataManifest for every cluster UUID
     * @return List of cluster UUIDs. The first element is the most recent cluster UUID in the chain
     */
    private List<String> createClusterChain(final Map<String, ClusterMetadataManifest> manifestsByClusterUUID) {
        final Map<String, String> clusterUUIDGraph = manifestsByClusterUUID.values()
            .stream()
            .collect(Collectors.toMap(ClusterMetadataManifest::getClusterUUID, ClusterMetadataManifest::getPreviousClusterUUID));
        final List<String> validClusterUUIDs = manifestsByClusterUUID.values()
            .stream()
            .filter(m -> !isInvalidClusterUUID(m) && !clusterUUIDGraph.containsValue(m.getClusterUUID()))
            .map(ClusterMetadataManifest::getClusterUUID)
            .collect(Collectors.toList());
        if (validClusterUUIDs.isEmpty()) {
            logger.info("There is no valid previous cluster UUID");
            return Collections.emptyList();
        }
        if (validClusterUUIDs.size() > 1) {
            throw new IllegalStateException(
                String.format(
                    Locale.ROOT,
                    "The system has ended into multiple valid cluster states in the remote store. "
                        + "Please check their latest manifest to decide which one you want to keep. Valid Cluster UUIDs: - %s",
                    validClusterUUIDs
                )
            );
        }
        final List<String> validChain = new ArrayList<>();
        String currentUUID = validClusterUUIDs.get(0);
        while (!ClusterState.UNKNOWN_UUID.equals(currentUUID)) {
            validChain.add(currentUUID);
            // Getting the previous cluster UUID of a cluster UUID from the clusterUUID Graph
            currentUUID = clusterUUIDGraph.get(currentUUID);
        }
        return validChain;
    }

    private boolean isInvalidClusterUUID(ClusterMetadataManifest manifest) {
        return !manifest.isCommitted() && manifest.getIndices().isEmpty();
    }

    /**
     * Fetch latest ClusterMetadataManifest file from remote state store
     *
     * @param clusterUUID uuid of cluster state to refer to in remote
     * @param clusterName name of the cluster
     * @return latest ClusterMetadataManifest filename
     */
    private Optional<String> getLatestManifestFileName(String clusterName, String clusterUUID) throws IllegalStateException {
        try {
            /**
             * {@link BlobContainer#listBlobsByPrefixInSortedOrder} will get the latest manifest file
             * as the manifest file name generated via {@link RemoteClusterStateService#getManifestFileName} ensures
             * when sorted in LEXICOGRAPHIC order the latest uploaded manifest file comes on top.
             */
            List<BlobMetadata> manifestFilesMetadata = manifestContainer(clusterName, clusterUUID).listBlobsByPrefixInSortedOrder(
                MANIFEST_FILE_PREFIX + DELIMITER,
                1,
                BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC
            );
            if (manifestFilesMetadata != null && !manifestFilesMetadata.isEmpty()) {
                return Optional.of(manifestFilesMetadata.get(0).name());
            }
        } catch (IOException e) {
            throw new IllegalStateException("Error while fetching latest manifest file for remote cluster state", e);
        }
        logger.info("No manifest file present in remote store for cluster name: {}, cluster UUID: {}", clusterName, clusterUUID);
        return Optional.empty();
    }

    /**
     * Fetch ClusterMetadataManifest from remote state store
     *
     * @param clusterUUID uuid of cluster state to refer to in remote
     * @param clusterName name of the cluster
     * @return ClusterMetadataManifest
     */
    private ClusterMetadataManifest fetchRemoteClusterMetadataManifest(String clusterName, String clusterUUID, String filename)
        throws IllegalStateException {
        try {
            return RemoteClusterStateService.CLUSTER_METADATA_MANIFEST_FORMAT.read(
                manifestContainer(clusterName, clusterUUID),
                filename,
                blobStoreRepository.getNamedXContentRegistry()
            );
        } catch (IOException e) {
            throw new IllegalStateException(String.format(Locale.ROOT, "Error while downloading cluster metadata - %s", filename), e);
        }
    }

    public static String encodeString(String content) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(content.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Exception for IndexMetadata transfer failures to remote
     */
    static class IndexMetadataTransferException extends RuntimeException {

        public IndexMetadataTransferException(String errorDesc) {
            super(errorDesc);
        }

        public IndexMetadataTransferException(String errorDesc, Throwable cause) {
            super(errorDesc, cause);
        }
    }

    /**
     * Purges all remote cluster state against provided cluster UUIDs
     * @param clusterName name of the cluster
     * @param clusterUUIDs clusteUUIDs for which the remote state needs to be purged
     */
    public void deleteStaleClusterMetadata(String clusterName, List<String> clusterUUIDs) {
        clusterUUIDs.forEach(clusterUUID -> {
            getBlobStoreTransferService().deleteAsync(
                ThreadPool.Names.REMOTE_PURGE,
                getCusterMetadataBasePath(clusterName, clusterUUID),
                new ActionListener<>() {
                    @Override
                    public void onResponse(Void unused) {
                        logger.info("Deleted all remote cluster metadata for cluster UUID - {}", clusterUUID);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.error(
                            new ParameterizedMessage(
                                "Exception occurred while deleting all remote cluster metadata for cluster UUID {}",
                                clusterUUID
                            ),
                            e
                        );
                    }
                }
            );
        });
    }

    /**
     * Deletes older than last {@code versionsToRetain} manifests. Also cleans up unreferenced IndexMetadata associated with older manifests
     * @param clusterName name of the cluster
     * @param clusterUUID uuid of cluster state to refer to in remote
     * @param manifestsToRetain no of latest manifest files to keep in remote
     */
    private void deleteStaleClusterMetadata(String clusterName, String clusterUUID, int manifestsToRetain) {
        if (deleteStaleMetadataRunning.compareAndSet(false, true) == false) {
            logger.info("Delete stale cluster metadata task is already in progress.");
            return;
        }
        try {
            getBlobStoreTransferService().listAllInSortedOrderAsync(
                ThreadPool.Names.REMOTE_PURGE,
                getManifestFolderPath(clusterName, clusterUUID),
                "manifest",
                Integer.MAX_VALUE,
                new ActionListener<>() {
                    @Override
                    public void onResponse(List<BlobMetadata> blobMetadata) {
                        if (blobMetadata.size() > manifestsToRetain) {
                            deleteClusterMetadata(
                                clusterName,
                                clusterUUID,
                                blobMetadata.subList(0, manifestsToRetain - 1),
                                blobMetadata.subList(manifestsToRetain - 1, blobMetadata.size())
                            );
                        }
                        deleteStaleMetadataRunning.set(false);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.error(
                            new ParameterizedMessage(
                                "Exception occurred while deleting Remote Cluster Metadata for clusterUUIDs {}",
                                clusterUUID
                            )
                        );
                        deleteStaleMetadataRunning.set(false);
                    }
                }
            );
        } finally {
            deleteStaleMetadataRunning.set(false);
        }
    }

    private void deleteClusterMetadata(
        String clusterName,
        String clusterUUID,
        List<BlobMetadata> activeManifestBlobMetadata,
        List<BlobMetadata> staleManifestBlobMetadata
    ) {
        try {
            Set<String> filesToKeep = new HashSet<>();
            Set<String> staleManifestPaths = new HashSet<>();
            Set<String> staleIndexMetadataPaths = new HashSet<>();
            activeManifestBlobMetadata.forEach(blobMetadata -> {
                ClusterMetadataManifest clusterMetadataManifest = fetchRemoteClusterMetadataManifest(
                    clusterName,
                    clusterUUID,
                    blobMetadata.name()
                );
                clusterMetadataManifest.getIndices()
                    .forEach(uploadedIndexMetadata -> filesToKeep.add(uploadedIndexMetadata.getUploadedFilename()));
            });
            staleManifestBlobMetadata.forEach(blobMetadata -> {
                ClusterMetadataManifest clusterMetadataManifest = fetchRemoteClusterMetadataManifest(
                    clusterName,
                    clusterUUID,
                    blobMetadata.name()
                );
                staleManifestPaths.add(new BlobPath().add(MANIFEST_PATH_TOKEN).buildAsString() + blobMetadata.name());
                clusterMetadataManifest.getIndices().forEach(uploadedIndexMetadata -> {
                    if (filesToKeep.contains(uploadedIndexMetadata.getUploadedFilename()) == false) {
                        staleIndexMetadataPaths.add(
                            new BlobPath().add(INDEX_PATH_TOKEN).add(uploadedIndexMetadata.getIndexUUID()).buildAsString()
                                + uploadedIndexMetadata.getUploadedFilename()
                                + ".dat"
                        );
                    }
                });
            });

            if (staleManifestPaths.isEmpty()) {
                logger.info("No stale Remote Cluster Metadata files found");
                return;
            }

            deleteStalePaths(clusterName, clusterUUID, new ArrayList<>(staleIndexMetadataPaths));
            deleteStalePaths(clusterName, clusterUUID, new ArrayList<>(staleManifestPaths));
        } catch (IllegalStateException e) {
            logger.error("Error while fetching Remote Cluster Metadata manifests", e);
        } catch (IOException e) {
            logger.error("Error while deleting stale Remote Cluster Metadata files", e);
        } catch (Exception e) {
            logger.error("Unexpected error while deleting stale Remote Cluster Metadata files", e);
        }
    }

    private void deleteStalePaths(String clusterName, String clusterUUID, List<String> stalePaths) throws IOException {
        logger.debug(String.format(Locale.ROOT, "Deleting stale files from remote - %s", stalePaths));
        getBlobStoreTransferService().deleteBlobs(getCusterMetadataBasePath(clusterName, clusterUUID), stalePaths);
    }
}
