package org.reactorlabs.jshealth.models;

/**
 * @param url is the project URL of the file.
 * @param fileType is the type of the file.
 * @param fileHash is the hash of the file.
 * @param commitId is commit id of the file.
 * @param commitMsg is comment for the commit.
 * @param commitTime is time file is committed.
 * @param branch is the branch of file.
 *
 * @author shabbir.ahussain
 */
case class FileHashTuple(url: String,
                         fileType: FileTypes.Value,
                         fileHash: String,
                         commitId: String = null,
                         commitMsg: String = null,
                         commitTime: Long = 0,
                         branch: String = "master"){
  override def toString: String = {
    "{\n\t url: '" + url +
      "'\n\t fileType: '"   + fileType +
      "'\n\t fileHash: '"   + fileHash +
      "'\n\t commitId: '"   + commitId +
      "'\n\t commitMsg: '"  + commitMsg +
      "'\n\t commitTime: '" + commitTime +
      "'\n}"
  }
}
