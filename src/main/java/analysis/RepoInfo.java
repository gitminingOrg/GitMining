package analysis;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.bson.Document;

import utility.MongoInfo;
import utility.MysqlInfo;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;

public class RepoInfo {
	public static void main(String[] args) throws Exception {
		RepoInfo repoInfo = new RepoInfo();
		repoInfo.getIssueAndPull();
	}
	
	public void getIssueAndPull() throws Exception{
		MongoClient mongoClient = new MongoClient(MongoInfo.getMongoServerIp(),27017);
		MongoDatabase database = mongoClient.getDatabase("ghcrawlerV3");
		FindIterable<Document> issueIterable = database.getCollection(
				"issueandpull").find();
		Connection connection = MysqlInfo.getMysqlConnection();
		connection.setAutoCommit(false);
		String sql = "update repotest set open_issues = ?,closed_issues = ?,open_pull=?,closed_pull=? where full_name = ?";
		PreparedStatement stmt = connection.prepareStatement(sql);
		JsonParser parser = new JsonParser();
		for (Document document : issueIterable) {
			String json = document.toJson();
			JsonObject repoIssue = parser.parse(json).getAsJsonObject();
			int openIssue = repoIssue.get("openissue").getAsInt();
			int closedIssue = repoIssue.get("closedissue").getAsInt();
			int openPull = repoIssue.get("openpull").getAsInt();
			int closedPull = repoIssue.get("closedpull").getAsInt();
			String repoName = repoIssue.get("fn").getAsString();
			
			stmt.setInt(1, openIssue);
			stmt.setInt(2, closedIssue);
			stmt.setInt(3, openPull);
			stmt.setInt(4, closedPull);
			stmt.setString(5, repoName);
			
			stmt.execute();
		}
		connection.commit();
		connection.close();
		mongoClient.close();
	}

	/**
	 * fetch the repo info from mongo to mysql
	 * 
	 * @throws Exception
	 */
	public void getRepo() throws Exception {
		// fetch from mongo
		MongoClient mongoClient = new MongoClient(MongoInfo.getMongoServerIp(),
				27017);
		MongoDatabase database = mongoClient.getDatabase("ghcrawlerV3");
		FindIterable<Document> repoIterable = database.getCollection(
				"repo").find();

		// get mysql connection
		Connection connection = MysqlInfo.getMysqlConnection();
		// refresh update time
		String updateSql = "update updatetime set repo_update_time = ?";
		PreparedStatement updateStmt = connection.prepareStatement(updateSql);
		Date time = Calendar.getInstance().getTime();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		updateStmt.setString(1, sdf.format(time));
		updateStmt.execute();
		
		connection.setAutoCommit(false);
		String sql = "replace into repotest(id,full_name,description,fork,owner_id,owner_name,owner_type,create_time,push_time,update_time,stargazers,subscribers,fork_num) values(?,?,?,?,?,?,?,?,?,?,?,?,?)";
		PreparedStatement stmt = connection.prepareStatement(sql);

		JsonParser parser = new JsonParser();
		for (Document document : repoIterable) {
			String json = document.toJson();
			JsonObject repoJsonObject = parser.parse(json).getAsJsonObject();
			int id = repoJsonObject.get("id").getAsInt();
			stmt.setInt(1, id);
			
			String full_Name = repoJsonObject.get("full_name").getAsString();
			System.out.println(full_Name);


			stmt.setString(2, full_Name);
			String description = "";
			if(repoJsonObject.has("description") && !repoJsonObject.get("description").isJsonNull()){
				description = repoJsonObject.get("description")
						.getAsString();
			}
			stmt.setString(3, description);
			
			boolean fork = repoJsonObject.get("fork").getAsBoolean();
			int forkNum = fork ? 1 : 0;
			stmt.setInt(4, forkNum);
			
			int owner_id = repoJsonObject.get("owner").getAsJsonObject()
					.get("id").getAsInt();
			stmt.setInt(5, owner_id);
			
			String[] items = full_Name.split("/");
			String owner_name = items[0];
			stmt.setString(6, owner_name);
			
			String ownerType = repoJsonObject.get("owner").getAsJsonObject()
					.get("type").getAsString();
			int ot_num = 1;
			if (ownerType.equals("Organization")) {
				ot_num = 2;
			}
			stmt.setInt(7, ot_num);
			
			String createTime = repoJsonObject.get("created_at").getAsString();
			stmt.setString(8, createTime);
			
			String pushTime = "";
			if (repoJsonObject.has("pushed_at")
					&& !repoJsonObject.get("pushed_at").isJsonNull()) {
				pushTime = repoJsonObject.get("pushed_at").getAsString();
			}
			stmt.setString(9, pushTime);
			
			String updateTime = repoJsonObject.get("updated_at").getAsString();
			stmt.setString(10, updateTime);
			
			int starCount = repoJsonObject.get("stargazers_count").getAsInt();
			stmt.setInt(11, starCount);
			
			int subscriber = repoJsonObject.get("subscribers_count").getAsInt();
			stmt.setInt(12, subscriber);
			
			int forksCount = repoJsonObject.get("forks_count").getAsInt();
			stmt.setInt(13, forksCount);
			
			stmt.execute();
		}
		connection.commit();
		stmt.close();
		connection.close();
		mongoClient.close();
	}

	public void getContribution() throws Exception {
		//get mysql connection
		Connection connection = MysqlInfo.getMysqlConnection();
		connection.setAutoCommit(false);
		String conSql = "insert into contribution(user_id,repo_id) values(?,?);";
		PreparedStatement conStmt = connection.prepareStatement(conSql);
		String repoSql = "update repotest set contributor = ? where id = ?";
		PreparedStatement repoStmt = connection.prepareStatement(repoSql);
		
		//get repos from mongo
		MongoClient mongoClient = new MongoClient(MongoInfo.getMongoServerIp(),
				27017);
		MongoDatabase database = mongoClient.getDatabase("ghcrawlerV1");
		FindIterable<Document> repoIterable = database
				.getCollection("repository").find();
		JsonParser parser = new JsonParser();
		Map<String, Integer> repoMap = new HashMap<String, Integer>();
		for (Document document : repoIterable) {
			String json = document.toJson();
			JsonObject repoJsonObject = parser.parse(json).getAsJsonObject();
			int id = repoJsonObject.get("id").getAsInt();
			String full_name = repoJsonObject.get("full_name").getAsString();
			System.out.println(id);
			repoMap.put(full_name, id);
		}

		Map<Integer,Integer> contributorMap = new HashMap<Integer, Integer>();
		
		FindIterable<Document> contributeIterable = database.getCollection(
				"contributors").find();
		for (Document document : contributeIterable) {
			String json = document.toJson();
			JsonObject contriJsonObject = parser.parse(json).getAsJsonObject();
			int id = contriJsonObject.get("id").getAsInt();
			String repoName = contriJsonObject.get("fn").getAsString();
			int repo_id = repoMap.get(repoName);
			conStmt.setInt(1, id);
			conStmt.setInt(2, repo_id);
			conStmt.execute();
			
			if(contributorMap.containsKey(repo_id)){
				contributorMap.put(repo_id, contributorMap.get(repo_id)+1);
			}else{
				contributorMap.put(repo_id, 1);
			}
		}
		
		Set<Integer> keySet = contributorMap.keySet();
		for (Integer repoId : keySet) {
			int contri_count = contributorMap.get(repoId);
			repoStmt.setInt(1, contri_count);
			repoStmt.setInt(2, repoId);
			repoStmt.execute();
		}
		
		mongoClient.close();
		connection.commit();
		conStmt.close();
		repoStmt.close();
		connection.close();
	}

	public void getCollaborators() throws Exception {
		//get mysql connection
		Connection connection = MysqlInfo.getMysqlConnection();
		connection.setAutoCommit(false);
		String conSql = "insert into collaborator(user_id,repo_id) values(?,?);";
		PreparedStatement conStmt = connection.prepareStatement(conSql);
		String repoSql = "update repotest set collaborator = ? where id = ?";
		PreparedStatement repoStmt = connection.prepareStatement(repoSql);
		
		//get repos from mongo
		MongoClient mongoClient = new MongoClient(MongoInfo.getMongoServerIp(),
				27017);
		MongoDatabase database = mongoClient.getDatabase("ghcrawlerV1");
		FindIterable<Document> repoIterable = database
				.getCollection("repository").find();
		JsonParser parser = new JsonParser();
		Map<String, Integer> repoMap = new HashMap<String, Integer>();
		for (Document document : repoIterable) {
			String json = document.toJson();
			JsonObject repoJsonObject = parser.parse(json).getAsJsonObject();
			int id = repoJsonObject.get("id").getAsInt();
			String full_name = repoJsonObject.get("full_name").getAsString();
			System.out.println(id);
			repoMap.put(full_name, id);
		}

		Map<Integer,Integer> collaboratorMap = new HashMap<Integer, Integer>();
		
		FindIterable<Document> collaboratorIterable = database.getCollection(
				"assignees").find();
		for (Document document : collaboratorIterable) {
			String json = document.toJson();
			JsonObject contriJsonObject = parser.parse(json).getAsJsonObject();
			int id = contriJsonObject.get("id").getAsInt();
			String repoName = contriJsonObject.get("fn").getAsString();
			int repo_id = repoMap.get(repoName);
			conStmt.setInt(1, id);
			conStmt.setInt(2, repo_id);
			conStmt.execute();
			
			if(collaboratorMap.containsKey(repo_id)){
				collaboratorMap.put(repo_id, collaboratorMap.get(repo_id)+1);
			}else{
				collaboratorMap.put(repo_id, 1);
			}
		}
		
		Set<Integer> keySet = collaboratorMap.keySet();
		for (Integer repoId : keySet) {
			int contri_count = collaboratorMap.get(repoId);
			repoStmt.setInt(1, contri_count);
			repoStmt.setInt(2, repoId);
			repoStmt.execute();
		}
		
		mongoClient.close();
		connection.commit();
		conStmt.close();
		repoStmt.close();
		connection.close();
	}
	
	
	public void analyseLanguage() throws Exception {
		//get mysql connection
		Connection connection = MysqlInfo.getMysqlConnection();
		connection.setAutoCommit(false);
		String lanSql = "insert into language(repo_id,language,count) values(?,?,?);";
		PreparedStatement lanStmt = connection.prepareStatement(lanSql);
		String repoSql = "update repotest set language = ? where id = ?";
		PreparedStatement repoStmt = connection.prepareStatement(repoSql);
		
		//get repos from mongo
		MongoClient mongoClient = new MongoClient(MongoInfo.getMongoServerIp(),
				27017);
		MongoDatabase database = mongoClient.getDatabase("ghcrawlerV1");
		FindIterable<Document> repoIterable = database
				.getCollection("repository").find();
		JsonParser parser = new JsonParser();
		Map<String, Integer> repoMap = new HashMap<String, Integer>();
		for (Document document : repoIterable) {
			String json = document.toJson();
			JsonObject repoJsonObject = parser.parse(json).getAsJsonObject();
			int id = repoJsonObject.get("id").getAsInt();
			String full_name = repoJsonObject.get("full_name").getAsString();
			System.out.println(id);
			repoMap.put(full_name, id);
		}

		Map<Integer,String> languageMap = new HashMap<Integer, String>();
		//the most language line of each repo
		Map<Integer,Integer> lanNumMap = new HashMap<Integer, Integer>();
		
		FindIterable<Document> collaboratorIterable = database.getCollection(
				"languages").find();
		for (Document document : collaboratorIterable) {
			String json = document.toJson();
			String[] items = json.split(",")[1].split(":");
			String language = items[0].trim().replaceAll("\"", "");
			int num = Integer.parseInt(items[1].trim());
			
			System.out.println(language +"\t" + num);
			JsonObject lanJsonObject = parser.parse(json).getAsJsonObject();
			String repoName = lanJsonObject.get("fn").getAsString();
			int repo_id = repoMap.get(repoName);
			
			if(lanNumMap.containsKey(repo_id)){
				if(num>=lanNumMap.get(repo_id)){
					languageMap.put(repo_id, language);
					lanNumMap.put(repo_id, num);
				}
			}else{
				languageMap.put(repo_id, language);
				lanNumMap.put(repo_id, num);				
			}
			lanStmt.setInt(1, repo_id);
			lanStmt.setString(2, language);
			lanStmt.setInt(3, num);
			lanStmt.execute();
		}
		
		Set<Integer> keySet = languageMap.keySet();
		for (Integer repoId : keySet) {
			String language = languageMap.get(repoId);
			repoStmt.setString(1, language);
			repoStmt.setInt(2, repoId);
			repoStmt.execute();
		}
		
		mongoClient.close();
		connection.commit();
		lanStmt.close();
		repoStmt.close();
		connection.close();
	}
	public void analyseContributors() {
		Map<String, Integer> repoMap = new HashMap<String, Integer>();
		JsonParser parser = new JsonParser();
		MongoClient mongoClient = new MongoClient(MongoInfo.getMongoServerIp(),
				27017);
		MongoDatabase database = mongoClient.getDatabase("ghcrawler");
		FindIterable<Document> contiIterable = database.getCollection(
				"contributors").find();
		for (Document document : contiIterable) {
			String json = document.toJson();
			JsonObject contributor = parser.parse(json).getAsJsonObject();
			String repoName = contributor.get("fn").getAsString();
			if (!repoMap.containsKey(repoName)) {
				repoMap.put(repoName, 1);
			} else {
				int conNum = repoMap.get(repoName);
				repoMap.put(repoName, conNum + 1);
			}
		}

		Set<String> keySet = repoMap.keySet();
		int index = 0;
		for (String key : keySet) {
			int num = repoMap.get(key);
			if (num > 1) {
				System.out.println(key + "\t" + num);
				index++;
			}
		}
		System.out.println(index);
		mongoClient.close();
	}

}
