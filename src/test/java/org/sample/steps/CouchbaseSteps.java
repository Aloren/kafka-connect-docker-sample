package org.sample.steps;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.playtika.test.couchbase.CouchbaseProperties;

public class CouchbaseSteps {

    private DefaultCouchbaseEnvironment env;
    private CouchbaseCluster cluster;
    private Bucket bucket;

    public Bucket createConnection(CouchbaseProperties couchbaseProperties) {
        env = DefaultCouchbaseEnvironment.builder()
                .bootstrapHttpDirectPort(couchbaseProperties.getHttpDirectPort())
                .bootstrapCarrierDirectPort(couchbaseProperties.getCarrierDirectPort())
                .kvTimeout(1_000)
                .connectTimeout(5_000)
                .build();

        cluster = CouchbaseCluster.create(env, couchbaseProperties.getHost());

        bucket = cluster.openBucket(couchbaseProperties.getBucket(), couchbaseProperties.getPassword());
        return bucket;
    }

    public void shutdownConnection() {
        if (env != null)
            env.shutdown();
        if (cluster != null)
            cluster.disconnect();
        if (bucket != null)
            bucket.close();
    }
}
