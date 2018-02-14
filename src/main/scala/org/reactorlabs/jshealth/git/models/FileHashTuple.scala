package org.reactorlabs.jshealth.git.models;

/**
 * @param url is the project URL of the file.
 * @param fileType is the type of the file.
 * @param fileHash is the hash of the file.
 * @param commitId is commit id of the file.
 * @param createTime is the time represents of the created date.
 *
 * @author shabbir.ahussain
 */
case class FileHashTuple(url: String,
                         fileType: FileTypes.Value,
                         fileHash: String,
                         commitId: String = "master",
                         createTime: Long = 0)
