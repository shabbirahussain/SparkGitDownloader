package org.reactorlabs.jshealth.models;

/**
 * @param owner is the owner of the repository.
 * @param repo is the name of the repository.
 * @param branch is the branch of file.
 * @param gitPath is the project URL of the file.
 * @param fileHash is the hash of the file.
 * @param error is a custom field carrying error message.
 * @param commitId is commit id of the file.
 * @param shortMsg is commit message of the file.
 * @param commitTime is time file is committed.
 * @param contents is the contents of the file.
 *
 * @author shabbirahussain
 */
case class FileHashTuple(owner:     String,
                         repo:      String,
                         branch:    String = "master",
                         gitPath:   String = "",
                         fileHash:  String = null,
                         error:     String = null,
                         commitId:  String = null,
                         shortMsg:  String = null,
                         commitTime:Long = 0,
                         contents:  String = null){
  override def toString
  : String = {
    "{" +
      " \n\t owner: '"      + owner +
      "'\n\t repo: '"       + repo +
      "'\n\t branch: '"     + branch +
      "'\n\t gitPath: '"    + gitPath +
      "'\n\t fileHash: '"   + fileHash +
      "'\n\t error: '"      + error +
      "'\n\t commitId: '"   + commitId +
      "'\n\t commitTime: '" + commitTime +
      "'\n\t contentsEmpty: '"+ contents.isEmpty +
      "'\n}"
  }
}
