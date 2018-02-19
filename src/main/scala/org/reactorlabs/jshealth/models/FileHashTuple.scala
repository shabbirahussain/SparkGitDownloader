package org.reactorlabs.jshealth.models;

/**
 * @param owner is the owner of the repository.
 * @param repo is the name of the repository.
 * @param branch is the branch of file.
 * @param gitPath is the project URL of the file.
 * @param fileType is the type of the file.
 * @param fileHash is the hash of the file.
 * @param commitId is commit id of the file.
 * @param commitMsg is comment for the commit.
 * @param commitTime is time file is committed.
 *
 * @author shabbirahussain
 */
case class FileHashTuple(owner:     String,
                         repo:      String,
                         branch:    String = "master",
                         gitPath:   String = "",
                         fileType:  FileTypes.Value = null,
                         var fileHash:  String = null,
                         error:     String = null,
                         commitId:  String = null,
                         commitMsg: String = null,
                         commitTime:Long = 0,
                         var byteSize:  Long = 0){
  override def toString
  : String = {
    "{" +
      " \n\t owner: '"      + owner +
      "'\n\t repo: '"       + repo +
      "'\n\t branch: '"     + branch +
      "'\n\t gitPath: '"    + gitPath +
      "'\n\t fileType: '"   + fileType +
      "'\n\t fileHash: '"   + fileHash +
      "'\n\t error: '"      + error +
      "'\n\t commitId: '"   + commitId +
      "'\n\t commitMsg: '"  + commitMsg +
      "'\n\t commitTime: '" + commitTime +
      "'\n}"
  }
}
