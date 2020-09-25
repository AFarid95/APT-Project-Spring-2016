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
import java.util.Dictionary;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author VAIO
 */
public class QueryProcessor {
    
    private Connection conn = null;
    
    public QueryProcessor(Connection conn)
    {
        this.conn=conn;
    }
    
    public int[][] process(String query, boolean isPhrase)
    {
        String[] wordsInQuery=query.split("\\W+");
        if(isPhrase)
        {
            //1st element in wordsInQuery is an empty string
            String[] wordsInPhrase=new String[wordsInQuery.length-1];
            for(int i=0;i<wordsInPhrase.length;i++)
                wordsInPhrase[i]=wordsInQuery[i+1];
            return phraseSearch(wordsInPhrase);
        }
        return normalSearch(wordsInQuery);
    }
    
    private int[][] normalSearch(String[] wordsInQuery)
    {
        String sqlStatement = "select distinct top "+Servlet.getMaxWebpages()
                    + " Occurences.WebpageID "
                    + "from Occurences,Word,Webpage "
                    + "where Occurences.WordID=Word.WordID "
                    + "and Occurences.WebpageID=Webpage.WebpageID "
                    + "and Filename is not null "
                    + "and (";
        for(int i=0;true;i++)
        {
            sqlStatement+="WordCol='"+stem(wordsInQuery[i])+"'";
            if(i==wordsInQuery.length-1)
                break;
            sqlStatement+=" or ";
        }
        sqlStatement+=")";
        try {
            PreparedStatement preparedStatement = conn.prepareStatement(sqlStatement,
                    ResultSet.TYPE_SCROLL_INSENSITIVE, 
                    ResultSet.CONCUR_READ_ONLY);
            ResultSet rs = preparedStatement.executeQuery();
            rs.last();
            int[][] webpages=new int[rs.getRow()][1];
            rs.beforeFirst();
            for(int i=0;rs.next();i++)
                webpages[i][0]=rs.getInt(1);
            return webpages;
        } catch (SQLException ex) {
            Logger.getLogger(QueryProcessor.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }
    }
    
    private int[][] phraseSearch(String[] wordsInQuery)
    {
        try {
            PreparedStatement preparedStatement = conn.prepareStatement("select "
                    + "Positions.WebpageID"
                    + ",Position"
                    + ",Number "
                    + "from Positions,Word,Webpage "
                    + "where Positions.WordID=Word.WordID "
                    + "and Positions.WebpageID=Webpage.WebpageID "
                    + "and Filename is not null "
                    + "and WordCol='"+stem(wordsInQuery[0])+"'");
            ResultSet rs1 = preparedStatement.executeQuery(); //all positions of 1st word in phrase
            int currentWebpage;
            String currentPosition;
            int currentNumber;
            int rowCount;
            ArrayList<Integer> webpagesList=new ArrayList<>();
            ArrayList<Integer> occurencesList=new ArrayList<>();
            Map<String,Integer> positionWeight=new HashMap();
            positionWeight.put("title",3);
            positionWeight.put("heading",2);
            positionWeight.put("paragraph",1);
            positionWeight.put("table",1);
            positionWeight.put("list",1);
            positionWeight.put("link",1);
            while(rs1.next())
            {
                currentWebpage=rs1.getInt("WebpageID");
                currentPosition=rs1.getNString("Position");
                currentNumber=rs1.getInt("Number");
                String sqlStatement="select Positions.WebpageID "
                        + "from Positions,Word,Webpage "
                        + "where Positions.WordID=Word.WordID "
                        + "and Positions.WebpageID=Webpage.WebpageID "
                        + "and Filename is not null "
                        + "and Positions.WebpageID="+currentWebpage+" "
                        + "and Position='"+currentPosition+"' "
                        + "and (";
                for(int i=0;true;i++)
                {
                    sqlStatement+="WordCol='"+stem(wordsInQuery[i])+"' "
                            + "and Number="+(currentNumber+i);
                    if(i>=wordsInQuery.length-1)
                        break;
                    sqlStatement+=" or ";
                }
                sqlStatement+=")";
                preparedStatement = conn.prepareStatement(sqlStatement,
                    ResultSet.TYPE_SCROLL_INSENSITIVE, 
                    ResultSet.CONCUR_READ_ONLY);
                ResultSet rs2 = preparedStatement.executeQuery();
                rs2.last();
                rowCount=rs2.getRow();
                rs2.beforeFirst();
                if(rowCount==wordsInQuery.length)
                {
                    if(!webpagesList.contains(currentWebpage))
                    {
                        webpagesList.add(currentWebpage);
                        occurencesList.add(positionWeight.getOrDefault(currentPosition,1)*1);
                        if(webpagesList.size()>=Servlet.getMaxWebpages())
                            break;
                    }
                    else
                    {
                        int newValue=occurencesList.get(occurencesList.size()-1)
                                +positionWeight.getOrDefault(currentPosition,1)*1;
                        occurencesList.set(newValue,rowCount);
                    }
                }
            }
            if(webpagesList.isEmpty())
                return null;
            else
            {
                int[][] webpages=new int[webpagesList.size()][2];
                Iterator<Integer> iterator = webpagesList.iterator();
                for (int[] webpage : webpages) {
                    webpage[0] = iterator.next();
                }
                iterator = occurencesList.iterator();
                for (int[] occurence : webpages) {
                    occurence[1] = iterator.next();
                }
                return webpages;
            }
        } catch (SQLException ex) {
            Logger.getLogger(QueryProcessor.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }
    }
    
    private static String stem(String str)
    {
        Stemmer s=new Stemmer();
        s.add(str.toCharArray(), str.length());
        s.stem();
        return s.toString();
    }
}
