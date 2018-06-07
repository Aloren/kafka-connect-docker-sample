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
        if (env != null) {
            try {
                env.shutdown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (cluster != null) {
            try {
                cluster.disconnect();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (bucket != null) {
            try {
                bucket.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
