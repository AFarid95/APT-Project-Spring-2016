/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package SearchEnginePhase2;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;


public class Ranker {
    
    private Connection conn = null;
    
    public Ranker(Connection conn)
    {
        this.conn=conn;
    }
    
    public int[] rank(int[][] webpages, String query, boolean isPhrase)
    {
        if(isPhrase)
            return phraseRank(webpages);
        String[] wordsInQuery=query.split("\\W+");
        return normalRank(webpages,wordsInQuery);
    }
    
    private int[] normalRank(int[][] webpages, String[] wordsInQuery)
    {
         int wordID;
         double wordindoc;
         double totalwordsindoc;
         double indexedpages;
         double pageswithword;
         PreparedStatement preparedStatement;
         ResultSet rs;
         double TF;
         double IDF;
         double maxpop=5;
         List orderedlist = new ArrayList();
         String query;
         double [] ranks=new double [webpages.length];
         for(int i=0;i<webpages.length;i++){
             ranks[i]=0;
         }
         int [] ordered;
         
         
        try {
                 query="select max(Popularity) from Webpage";
                 preparedStatement = conn.prepareStatement(query);
                 rs = preparedStatement.executeQuery();  
                 rs.next();
                 maxpop=rs.getDouble(1);
        } catch (SQLException ex) {
            Logger.getLogger(Ranker.class.getName()).log(Level.SEVERE, null, ex);
        }

  
        for (String wordsInQuery1 : wordsInQuery) {
            try {
                query = "select WordID from Word where WordCol='" + stem(wordsInQuery1) + "'";
                preparedStatement = conn.prepareStatement(query);
                rs = preparedStatement.executeQuery(); 
                rs.next();
                wordID=rs.getInt(1);
                query="select count(WebpageID) from Webpage where Indexed=1";
                preparedStatement = conn.prepareStatement(query);
                rs = preparedStatement.executeQuery();
                rs.next();
                indexedpages=rs.getInt(1);
                query="select count(w.WebpageID) from Occurences o,Webpage w where w.Indexed=1 and o.WebpageID=w.WebpageID and o.WordID="+wordID;
                preparedStatement = conn.prepareStatement(query);
                rs = preparedStatement.executeQuery();
                rs.next();
                pageswithword=rs.getInt(1);
                double x=((indexedpages)/(pageswithword));
                IDF=Math.log10((x));
                for(int j=0;j<webpages.length;j++){
                    query="select WordsInTitle,WordsInHeadings,OtherWords from Occurences where WordID="+wordID+" and WebpageID="+webpages[j][0];
                    preparedStatement = conn.prepareStatement(query);
                    rs = preparedStatement.executeQuery();
                    if(rs.next()!= false) {
                        wordindoc=(4*rs.getInt("WordsInTitle"))+(2*rs.getInt("WordsInHeadings"))+(rs.getInt("OtherWords"));
                        query="select sum(WordsInTitle) as title,sum(WordsInHeadings) as heading,sum(OtherWords) as others from occurences where WebpageID="+webpages[j][0];
                        preparedStatement = conn.prepareStatement(query);
                        rs = preparedStatement.executeQuery();
                        rs.next();
                        totalwordsindoc=(4*rs.getInt("title"))+(2*rs.getInt("heading"))+(rs.getInt("others"));
                        TF=wordindoc/totalwordsindoc;
                    }
                    else{
                        TF=0;
                    }
                    ranks[j]=ranks[j]+(TF*IDF);
                    
                }
            }catch (SQLException ex) {
                Logger.getLogger(Ranker.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        for(int i=0;i<webpages.length;i++){
         
             try {
                 query="select Popularity from Webpage where WebpageID="+webpages[i][0];
                 preparedStatement = conn.prepareStatement(query);
                 rs = preparedStatement.executeQuery();  
                 rs.next();
                 ranks[i]*=(((rs.getDouble(1))/maxpop)*5);
             } catch (SQLException ex) {
                 Logger.getLogger(Ranker.class.getName()).log(Level.SEVERE, null, ex);
             }
                 
                 }
       
               try {
                   
               for(int i=0;i<webpages.length;i++){
                   
            query="insert into Rank Values ("+webpages[i][0]+","+ranks[i]+")";
                 preparedStatement = conn.prepareStatement(query);
                 preparedStatement.executeUpdate();
                 preparedStatement.close();
               }
               
            query="select webpageID from Rank order by poprelevance desc";
                 preparedStatement = conn.prepareStatement(query);
                 rs = preparedStatement.executeQuery();
                 while (rs.next()) {
                     orderedlist.add(rs.getInt(1));
                 } 
                 query="delete from Rank";
                 preparedStatement = conn.prepareStatement(query);
                 preparedStatement.executeUpdate();
                 
                 ordered=new int[orderedlist.size()];
                 for(int i=0;i<orderedlist.size();i++){
                     ordered[i]=(int)orderedlist.get(i);
                 }
               return ordered;
                 
             } catch (SQLException ex) {
                 Logger.getLogger(Ranker.class.getName()).log(Level.SEVERE, null, ex);
             }
               ordered=new int[orderedlist.size()];
    return ordered;
        }
    

    private int[] phraseRank(int[][] webpages)
    {
       double IDF; 
       String query;
       PreparedStatement preparedStatement;
       ResultSet rs;
       double indexedpages;
       double pageswithphrase;
       double totalwordsindoc;         
       double TF;
       double maxpop=5;
       List orderedlist = new ArrayList();
       int [] ordered;
       double [] ranks=new double[webpages.length];
        try {
             query="select max(Popularity) from Webpage";
             preparedStatement = conn.prepareStatement(query);
             rs = preparedStatement.executeQuery();  
             rs.next();
             maxpop=rs.getDouble(1);
            
            query="select count(WebpageID) from Webpage where Indexed=1";
            preparedStatement = conn.prepareStatement(query);
            rs = preparedStatement.executeQuery();
            rs.next();
            indexedpages=rs.getDouble(1);
            pageswithphrase=webpages.length;
            IDF=Math.log10((indexedpages/pageswithphrase));
            
            for(int i=0;i<webpages.length;i++){
            query="select sum(TotalWords)from occurences where WebpageID="+webpages[i][0];
            preparedStatement = conn.prepareStatement(query);
            rs = preparedStatement.executeQuery();
            rs.next();
            totalwordsindoc=rs.getInt(1);
            TF=(webpages[i][1])/totalwordsindoc;
                    
                 query="select Popularity from Webpage where WebpageID="+webpages[i][0];
                 preparedStatement = conn.prepareStatement(query);
                 rs = preparedStatement.executeQuery();  
                 rs.next();
                 ranks[i]=((((rs.getDouble(1))/maxpop)*5)*(TF*IDF));
                 
            }
            for(int i=0;i<webpages.length;i++){
            query="insert into Rank Values ("+webpages[i][0]+","+ranks[i]+")";
                 preparedStatement = conn.prepareStatement(query);
                 preparedStatement.executeUpdate();
               }
               
            query="select webpageID from Rank order by poprelevance desc";
                 preparedStatement = conn.prepareStatement(query);
                 rs = preparedStatement.executeQuery();
                 while (rs.next()) {
                     orderedlist.add(rs.getInt(1));
                 } 
                 query="delete from Rank";
                 preparedStatement = conn.prepareStatement(query);
                 preparedStatement.executeUpdate();
        } catch (SQLException ex) {
            Logger.getLogger(Ranker.class.getName()).log(Level.SEVERE, null, ex);
        }
              ordered=new int[orderedlist.size()];
                 for(int i=0;i<orderedlist.size();i++){
                     ordered[i]=(int)orderedlist.get(i);
                 }
              return ordered;
    }
    
    private static String stem(String str)
    {
        Stemmer s=new Stemmer();
        s.add(str.toCharArray(), str.length());
        s.stem();
        return s.toString();
    }
    
}
