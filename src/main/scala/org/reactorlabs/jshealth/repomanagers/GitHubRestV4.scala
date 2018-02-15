package org.reactorlabs.jshealth.repomanagers

import java.text.SimpleDateFormat

import com.google.gson.Gson
import org.apache.http.client.methods.{CloseableHttpResponse, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.log4j.Level
import org.reactorlabs.jshealth.Main.logger
import org.reactorlabs.jshealth.datastores.Keychain
import org.reactorlabs.jshealth.models.{FileHashTuple, FileTypes}

import scala.io.Source

/** Class responsible for downloading files from github.com. It serves as a wrapper over Git API.
  *
  * @author shabbirahussain
  */
@SerialVersionUID(100L)
class GitHubRestV4(apiKeysPath: String, maxRetries: Int = 1)
  extends RepoManager with Serializable {
  private val gitApiEndpoint: String = "https://api.github.com/graphql"
  private val gson     = new Gson()
  private val keychain = new Keychain(apiKeysPath)
  private val df       = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")

  private var errCnt   : Int = 0
  private var apiKey   : String = _
  private var remaining: Int = 0
  private var reset    : Long = 0
  private var isValid  : Boolean = false
  /** Executes the given query and validates api keys. Also attempts to for common errors.
    *
    * @param query is the query to execute.
    * @return Only OK http responses, otherwise throws an exceptions if max retry attempts are reached.
    */
  private def execQuery(query: Query): CloseableHttpResponse = {
    apiKey  = keychain.getNextKey(apiKey, remaining, reset, isValid)

    // Wait for a valid api key.
    if (apiKey == null){
      incError()
      val minWait = Math.max(1, keychain.getMinCooldownTime - System.currentTimeMillis())
      Thread.sleep(minWait)
      return execQuery(query)
    }

    // Fire the request in sync mode.
    val post = new HttpPost(gitApiEndpoint)
    post.setHeader("Content-type", "application/json")
    post.setHeader("Authorization", "bearer " + apiKey)
    post.setEntity(new StringEntity(gson.toJson(query)))

    val response = parseResponse(HttpClientBuilder.create.build.execute(post))
    if (response.isDefined) return response.get
    execQuery(query)
  }

  /** Checks for common errors and reties. Throws exception if max reties are exhausted.
    *
    * @param response is the HTTP response from the request.
    * @return HTTP response from one of the retries if all is well.
    */
  private def parseResponse(response: CloseableHttpResponse): Option[CloseableHttpResponse] = {
    response.getFirstHeader("Status").getValue match {
      case  "200 OK" => {
        errCnt  = 0

        response.getFirstHeader("X-RateLimit-Limit")
        isValid   = true
        remaining = response.getFirstHeader("X-RateLimit-Remaining").getValue.toInt
        reset     = response.getFirstHeader("X-RateLimit-Reset").getValue.toLong
        return Some(response)
      }
      case  "401 Unauthorized" => isValid = false
      case _ =>
    }
    incError()
    None
  }

  /**
    * Tracks error count. Throws error if max errors allowed are reached.
    */
  private def incError(): Unit = {
    errCnt += 1
    if (errCnt >= maxRetries) {
      var msg = "Maximum retry attempts reached."
      logger.log(Level.ERROR, msg)
    }
  }

  private case class Query(query: String){
    override def toString = """{"query": "query """ + query + """"} """
  }

  // ===================== API Methods exposed =====================

  override def getProject(owner: String, repo:String, branch:String, gitPath: String): Unit = {

  }

  override def listFiles(owner: String, repo:String, branch:String, gitPath: String)
  : Seq[FileHashTuple] = {
    val path  = branch + ":"  + gitPath

    val query = Query(
      """{
        |  repository(owner: "%s", name: "%s") {
        |    object(expression: "%s") {
        |      ... on Tree {
        |        entries {
        |          name
        |          oid
        |          type
        |        }
        |      }
        |    }
        |  }
        |}
      """.stripMargin.format(owner, repo, path))
    val response = execQuery(query)
    val str = Source.fromInputStream(response.getEntity.getContent).mkString("")
    val jObj = new org.json.JSONObject(str)

    var res = Seq[FileHashTuple]()

    try{
      val entries = jObj.getJSONObject("data")
        .getJSONObject("repository")
        .getJSONObject("object")
        .getJSONArray("entries")

      res = (0 until entries.length).map(i => {
        val entry = entries.getJSONObject(i)

        val objUrl  = gitPath + (if (gitPath.nonEmpty) "/" else "") + entry.getString("name")
        val objId   = entry.getString("oid")
        val objType = entry.getString("type")

        FileHashTuple(owner = owner,
          repo  = repo,
          branch      = branch,
          gitPath         = objUrl,
          fileType    = FileTypes.withName(objType),
          fileHash    = objId)
      })
    } catch {case e:Exception => {println(query)}}
    res
  }

  override def getFileCommits(owner: String, repo:String, branch:String, gitPath: String)
  : Seq[FileHashTuple] = {
    val path  = gitPath

    val query = Query(
      """{
        |  repository(owner: "%s", name: "%s") {
        |    defaultBranchRef{
        |      target {
        |        ...on Commit{
        |            history(first:100,path: "%s"){
        |            nodes {
        |              message
        |              oid
        |              committedDate
        |            }
        |          }
        |        }
        |      }
        |    }
        |  }
        |}
      """.stripMargin.format(owner, repo, path))
    val response = execQuery(query)
    val str = Source.fromInputStream(response.getEntity.getContent).mkString("")
    val jObj = new org.json.JSONObject(str)

    var res = Seq[FileHashTuple]()
    try{
      val entries = jObj.getJSONObject("data")
        .getJSONObject("repository")
        .getJSONObject("defaultBranchRef")
        .getJSONObject("target")
        .getJSONObject("history")
        .getJSONArray("nodes")

      res = (0 until entries.length).map(i => {
        val entry = entries.getJSONObject(i)

        val objId     = entry.getString("oid")
        val commitMsg = entry.getString("message")
        val commitTime= df.parse(entry.getString("committedDate")).getTime

        FileHashTuple(
          owner = owner,
          repo = repo,
          gitPath       = gitPath,
          fileType  = null,
          fileHash  = objId,
          branch    = branch,
          commitMsg = commitMsg,
          commitTime= commitTime)
      })
    } catch {case e:Exception => {println(query)}}
    res
  }
}
