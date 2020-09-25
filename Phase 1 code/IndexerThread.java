/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package search.engine;

import java.io.File;
import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

/**
 *
 * @author VAIO
 */
public class IndexerThread implements Runnable{
    
    private String FILENAME;
    private static Connection conn;
    private static Stemmer s;
    private   int theOffset; //ramy
    private  int NoOfThreads; //ramy
    
    
    IndexerThread(String FILENAME,int Myoffset,int THREADSLenght)//ramy zawedt el two params offset w threadlength
    {
        this.FILENAME=FILENAME;
        this.theOffset=Myoffset;
        this.NoOfThreads=THREADSLenght;
        
    }
    
    public static void setconn(Connection conn)
    {
        IndexerThread.conn=conn;
    }
    
    public static void setStemmer(Stemmer s)
    {
        IndexerThread.s=s;
    }
    
    @Override
    public void run()
    {
        while(theOffset <Indexer.FILENAMES.size())//ramy
        {
            this.FILENAME=Indexer.FILENAMES.get(theOffset);//ramy
            
            
            System.out.println("Indexing of "+FILENAME+" has started");
            int webpageID=getWebpageID();
            File input = new File(FILENAME);
            Document doc=null;
            try {
                doc = Jsoup.parse(input, "UTF-8");
            } catch (IOException ex) {
                Logger.getLogger(Indexer.class.getName()).log(Level.SEVERE, null, ex);
            }
            if(doc!=null)
            {
                String[] wordsInTitle=doc.getElementsByTag("title").text().split("\\W+");
                String[][] wordsInHeadings=new String[6][];
                for(int i=0;i<6;i++)
                   wordsInHeadings[i]=doc.getElementsByTag("h"+(i+1)).text().split("\\W+");
                String[] wordsInParagraphs=doc.getElementsByTag("p").text().split("\\W+");
                String[] wordsInTables=doc.getElementsByTag("table").text().split("\\W+");
                String[] wordsInLists=doc.getElementsByTag("li").text().split("\\W+");
                String[] wordsInLinks=doc.getElementsByTag("a").text().split("\\W+");
                try {
                    CallableStatement insertWordProc
                            = conn.prepareCall("{call dbo.InsertWord(?,?,?,?)}");
    //                PreparedStatement insertNextWordStmt;
                    String stemmedWord;
                    int wordNumber=0;
    //                String previousWordInTitles="";
    //                String[] previousWordInHeadings=new String[6];
    //                for(String word : previousWordInHeadings)
    //                    word="";
    //                String previousWordInParagraphs="";
    //                String previousWordInTables="";
    //                String previousWordInLists="";
    //                String previousWordInLinks="";
                    insertWordProc.setNString(2,FILENAME);
                    insertWordProc.setNString(3,"title");
                    for (String word : wordsInTitle) {
                        wordNumber++;
                        stemmedWord=stem(word);
                        insertWordProc.setNString(1,stemmedWord);
                        insertWordProc.setInt(4,wordNumber);
                        insertWordProc.execute();
                    }
                    insertWordProc.setNString(3,"heading");
                    wordNumber=0;
                    for (String[] wordsInHeading : wordsInHeadings)
                    {
                        for (String word : wordsInHeading) {
                            wordNumber++;
                            stemmedWord=stem(word);
                            insertWordProc.setString(1,stemmedWord);
                            insertWordProc.setInt(4,wordNumber);
                            insertWordProc.execute();
                        }
                    }
                    insertWordProc.setNString(3,"paragraph");
                    wordNumber=0;
                    for (String word : wordsInParagraphs) {
                        wordNumber++;
                        stemmedWord=stem(word);
                        insertWordProc.setNString(1,stemmedWord);
                        insertWordProc.setInt(4,wordNumber);
                        insertWordProc.execute();
                    }
                    insertWordProc.setNString(3,"table");
                    wordNumber=0;
                    for (String word : wordsInTables) {
                        wordNumber++;
                        stemmedWord=stem(word);
                        insertWordProc.setNString(1,stemmedWord);
                        insertWordProc.setInt(4,wordNumber);
                        insertWordProc.execute();
                    }
                    insertWordProc.setNString(3,"list");
                    wordNumber=0;
                    for (String word : wordsInLists) {
                        wordNumber++;
                        stemmedWord=stem(word);
                        insertWordProc.setNString(1,stemmedWord);
                        insertWordProc.setInt(4,wordNumber);
                        insertWordProc.execute();
                    }
                    insertWordProc.setNString(3,"link");
                    wordNumber=0;
                    for (String word : wordsInLinks) {
                        wordNumber++;
                        stemmedWord=stem(word);
                        insertWordProc.setNString(1,stemmedWord);
                        insertWordProc.setInt(4,wordNumber);
                        insertWordProc.execute();
                    }
                } catch (SQLException ex) {
//                    Logger.getLogger(Indexer.class.getName()).log(Level.SEVERE, null, ex);
                }
                makeWebpageIndexed(FILENAME);
                System.out.println(FILENAME+" has been indexed successfully");

            }
//            System.out.println("will try to index another page");
            theOffset+=NoOfThreads; //ramy
        }//end of while //ramy
         System.out.println("Indexer thread exiting"); //ramy
    }
    
    private void makeWebpageIndexed(String filename) {
        String updateTableSQL = "update Webpage set Indexed=1 where [Filename]='"+filename+"'";
        PreparedStatement preparedStatement;
        try {
            preparedStatement = conn.prepareStatement(updateTableSQL);
            preparedStatement.executeUpdate();
        } catch (SQLException ex) {
            Logger.getLogger(CrawlerThread.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    private static String stem(String str)
    {
        s.add(str.toCharArray(), str.length());
        s.stem();
        return s.toString();
    }
    
    private int getWebpageID()
    {
        try {
            PreparedStatement preparedStatement = conn.prepareStatement("select WebpageID "
                    + "from Webpage "
                    + "where Filename='"+FILENAME+"'");
            ResultSet rs = preparedStatement.executeQuery();
            if(rs.next())
                return rs.getInt(1);
            else
                return 0;
        } catch (SQLException ex) {
            Logger.getLogger(CrawlerThread.class.getName()).log(Level.SEVERE, null, ex);
            return -1;
        }
    }
    
    private int getWordID(String word)
    {
        try {
            PreparedStatement preparedStatement = conn.prepareStatement("select WordID"
                    + "from Word"
                    + "where WordCol='"+word+"'");
            ResultSet rs = preparedStatement.executeQuery();
            return rs.getInt(1);
        } catch (SQLException ex) {
            Logger.getLogger(CrawlerThread.class.getName()).log(Level.SEVERE, null, ex);
            return -1;
        }
    }
}
