package dev.caraml.store.sparkjob.hash;

import dev.caraml.store.protobuf.jobservice.JobServiceProto.JobType;
import org.apache.commons.codec.digest.DigestUtils;

public class HashUtils {

  public static int JOB_ID_LENGTH = 16;

  public static String projectFeatureTableHash(String project, String tableName) {
    return DigestUtils.md5Hex(String.format("%s:%s", project, tableName));
  }

  public static String ingestionJobHash(
      JobType jobType, String project, String tableName, String storeName) {
    return DigestUtils.md5Hex(String.join("-", jobType.toString(), project, tableName, storeName))
        .substring(0, JOB_ID_LENGTH)
        .toLowerCase();
  }
}
