//AD-JUSTER HOMEWORK BRENT LEE

import org.apache.http.HttpEntity;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.BufferedReader;
import java.io.PrintWriter;
import java.io.FileWriter;
import java.io.FileOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import com.google.gson.Gson;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.DatabaseMetaData;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class adjusterclient {

    private static final Campaign[] emptyCampaign = {};
	private static final Creative[] emptyCreative = {};

    public static void main(String[] args) throws IOException, SQLException {
        
        String url = "jdbc:sqlite:./database.db";
        SQLite.createNewDatabase(url);

        //Creating TABLES
        String campaignTable = "CREATE TABLE IF NOT EXISTS campaigns (\n"
        + "	name text   NOT NULL  UNIQUE, \n"
        + " cpm text,\n"
        + "	id integer  UNIQUE,\n"
        + "	startDate text NOT NULL\n"
        + ")";

        String creativeTable = "CREATE TABLE IF NOT EXISTS creatives (\n"
        + "	parentId integer ,\n"
        + " clicks integer,\n"
        + "	id integer,\n"
        + "	views integer\n"
        + ")";

        SQLite.createNewTable(url,campaignTable);
        SQLite.createNewTable(url,creativeTable);

        Campaign[] myCampaigns = campaignGson();

        //Inserting into Campaigns
        String query = "INSERT INTO campaigns(name,cpm,id,startDate) VALUES(?,?,?,?)";
        try (Connection conn = DriverManager.getConnection(url);
                PreparedStatement pstmt = conn.prepareStatement(query)) {
            for(int i = 0; i < myCampaigns.length;i++)
            {
                pstmt.setString(1, myCampaigns[i].getName());
                pstmt.setString(2, myCampaigns[i].getCpm());
                pstmt.setInt(3, myCampaigns[i].getId());
                pstmt.setString(4, myCampaigns[i].getStartDate());
                pstmt.executeUpdate();            
            }
         } catch (SQLException e) {
                System.out.println(e.getMessage());
        }

        Creative[] myCreatives = creativeGson();

        //Inserting into Creatives
        query = "INSERT INTO creatives(parentId,clicks,id,views) VALUES(?,?,?,?)";
        try (Connection conn = DriverManager.getConnection(url);
                PreparedStatement pstmt = conn.prepareStatement(query)) {
            for(int i = 0; i < myCreatives.length; i++)
            {
                pstmt.setInt(1, myCreatives[i].getParentId());
                pstmt.setInt(2, myCreatives[i].getClicks());
                pstmt.setInt(3, myCreatives[i].getId());
                pstmt.setInt(4, myCreatives[i].getViews());
                pstmt.executeUpdate();
            }
         } catch (SQLException e) {
                System.out.println(e.getMessage());
        }

        query = "ALTER TABLE campaigns ADD COLUMN totalClicks integer";
        SQLite.sqliteQuery(url, query);

        query = "ALTER TABLE campaigns ADD COLUMN totalViews integer";
        SQLite.sqliteQuery(url, query);


        query = "UPDATE campaigns \n" + "SET totalClicks = (SELECT sum(creatives.clicks) from creatives WHERE\n" + "campaigns.id = creatives.parentId),\n"
            + "totalViews = (SELECT sum(creatives.views) from creatives WHERE\n"+ "campaigns.id = creatives.parentId)";
        SQLite.sqliteQuery(url, query);


        query = "ALTER TABLE campaigns ADD COLUMN revenue string";
        SQLite.sqliteQuery(url, query);
        query = "UPDATE campaigns\n" +"SET revenue = '$'|| cast((SELECT round(campaigns.totalViews*cast(substr(cpm, instr(cpm, '$') + 1) AS DOUBLE)/1000,2)) AS STRING)";
        SQLite.sqliteQuery(url, query);

        File outfile = new File("./database.csv");
        PrintWriter pw = new PrintWriter(new FileWriter(outfile));
        pw.write("id,name,cpm,startDate,totalClicks,totalViews,revenue\n");
        query = "SELECT id,name,cpm,startDate,totalClicks,totalViews,revenue from campaigns";

        String nextLine;
        String name;

        try (Connection conn = DriverManager.getConnection(url);
        PreparedStatement pstmt = conn.prepareStatement(query)) {
            ResultSet result = pstmt.executeQuery();
            while(result.next())
            {
                name = "\"" + result.getString("name") + "\"";
                nextLine = result.getInt("id") + "," + name + "," + result.getString("cpm") + "," + result.getString("startDate") + ","
                + result.getInt("totalClicks") + "," + result.getInt("totalViews") + "," + result.getString("revenue");
                pw.write(nextLine);
                pw.write("\n");
            }
        }   catch (SQLException e) {
                System.out.println(e.getMessage());
            }
            finally {
                pw.close();
            }
    }

    public static Campaign[] campaignGson(){
    
        CloseableHttpClient httpclient = HttpClients.createDefault();
        try {
            HttpGet httpGet = new HttpGet("http://homework.ad-juster.com/api/campaigns");
            CloseableHttpResponse response = httpclient.execute(httpGet);

            try {
                //System.out.println(response.getStatusLine());
                HttpEntity entity = response.getEntity();
                
                StringBuilder campaignString = new StringBuilder();
                BufferedReader buffRead = new BufferedReader(new InputStreamReader(entity.getContent()));
                
                String out;
                while((out = buffRead.readLine()) != null){
                    campaignString.append(out);
                }

                Gson gson = new Gson();
                return gson.fromJson(campaignString.toString(), Campaign[].class);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                response.close();
            }
            
        }
        catch (IOException e) {
			e.printStackTrace();
		}
         finally {
            try{
            httpclient.close();
            } catch (IOException e){
                e.printStackTrace();
            }
        }
        return emptyCampaign;
    }

    public static Creative[] creativeGson(){
    
        CloseableHttpClient httpclient = HttpClients.createDefault();
        try {
            HttpGet httpGet = new HttpGet("http://homework.ad-juster.com/api/creatives");
            CloseableHttpResponse response = httpclient.execute(httpGet);

            try {
                //System.out.println(response.getStatusLine());
                HttpEntity entity = response.getEntity();
                
                StringBuilder creativeString = new StringBuilder();
                BufferedReader buffRead = new BufferedReader(new InputStreamReader(entity.getContent()));
                
                String out;
                while((out = buffRead.readLine()) != null){
                    creativeString.append(out);
                }

                Gson gson = new Gson();
                return gson.fromJson(creativeString.toString(), Creative[].class);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                response.close();
            }
            
        }
        catch (IOException e) {
			e.printStackTrace();
		}
         finally {
            try{
            httpclient.close();
            } catch (IOException e){
                e.printStackTrace();
            }
        }
        return emptyCreative;
    }
}