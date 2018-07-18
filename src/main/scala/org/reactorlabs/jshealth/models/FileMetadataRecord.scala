package org.reactorlabs.jshealth.models

/**
 * @param repo_owner is the owner of the repository.
 * @param repository is the name of the repository.
 * @param branch is the branch of file.
 * @param git_path is the project URL of the file.
 * @param file_hash is the hash of the file.
 * @param commit_id is commit id of the file.
 * @param commit_time is time file is committed.
 *
 * @author shabbirahussain
 */
case class FileMetadataRecord(repo_owner: String,
                              repository: String,
                              branch:     String = "master",
                              git_path:   String = "",
                              file_hash:  String = null,
                              commit_id:  String = null,
                              commit_time:Long = 0){
  override def toString
  : String = {
    "{" +
      " \n\t owner: '"      + repo_owner +
      "'\n\t repo: '"       + repository +
      "'\n\t branch: '"     + branch +
      "'\n\t gitPath: '"    + git_path +
      "'\n\t fileHash: '"   + file_hash +
      "'\n\t commitId: '"   + commit_id +
      "'\n\t commitTime: '" + commit_time +
      "'\n}"
  }
}
