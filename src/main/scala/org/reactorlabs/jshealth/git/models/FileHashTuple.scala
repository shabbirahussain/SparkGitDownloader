package org.reactorlabs.jshealth.git.models;

/**
 * @param url is the project URL of the file.
 * @param commitId is commit id of the file.
 * @param createTime is the time represents of the created date.
 * @param fileHash is the hash of the file.
 *
 * @author shabbir.ahussain
 */
final case class FileHashTuple(url: String, commitId: String, createTime: Long, fileHash: String)