package org.reactorlabs.jshealth.models

/**
  * @param repo_owner is the owner of the repository.
  * @param repository is the name of the repository.
  * @param branch is the branch of file.
  * @param commit_id is commit id of the file.
  * @param commit_time is time file is committed.
  *
  * @author shabbirahussain
  */
case class CommitMessageRecord(repo_owner:String,
                              repository: String,
                              branch:     String,
                              author:     String,
                              commit_id:  String,
                              commit_time:Long,
                              commit_message: String){
  override def toString
  : String = {
    "{" +
      " \n\t owner: '"       + repo_owner +
      "'\n\t repo: '"        + repository +
      "'\n\t branch: '"      + branch +
      "'\n\t commitId: '"    + commit_id +
      "'\n\t commitTime: '"  + commit_time +
      "'\n\t commitMessage:" + commit_message +
      "'\n}"
  }
}
