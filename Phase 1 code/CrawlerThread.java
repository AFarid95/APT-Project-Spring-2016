 /*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package search.engine;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jsoup.*;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;


/**
 *
 * @author VAIO
 */
public class CrawlerThread implements Runnable{
    public static int MAX_DOCS;//max docs to add mwgood f class Crawler
    public static int counter=-1;
    
    private final java.sql.Connection conn;
    
    
    CrawlerThread(java.sql.Connection conn)
    {
        this.conn=conn;
    }
    
    @Override
    public void run()
    {
        Document doc = null;
        Elements links;
        boolean timeout;
        boolean allowed;
        int initialNumWebpagesSaved=getNumWebpagesSaved();
        for(Integer i=0;i<Crawler.URL_LIST.size();i++)
        {
            allowed=true;
            timeout=true;
            while(timeout)
            {
                try
                {
                    synchronized(Crawler.URL_LIST)
                    {
                        i=GetIndex(Thread.currentThread().getName());

                        if(i>=MAX_DOCS)//MaxDocs=Urlist initial size+ docsToAdd
                        {
                            System.out.println("Thread "+Thread.currentThread().getName()+
                            " ended at counter = "+i);
                            return;
                        }

                        System.out.println("Thread "+Thread.currentThread().getName()+
                            " will crawl link "+Crawler.URL_LIST.get(i)+" at index "+i );
                    }
                    allowed=robotIsAllowed(Crawler.URL_LIST.get(i));
                    if(allowed)
                        doc = Jsoup.connect(Crawler.URL_LIST.get(i))
                                .userAgent("Mozilla/5.0 "
                                        + "(Windows NT 6.1; WOW64) "
                                        + "AppleWebKit/537.36 "
                                        + "(KHTML, like Gecko) "
                                        + "Chrome/49.0.2623.110 Safari/537.36")
                                .get();
                    timeout=false;
                } catch (java.net.SocketTimeoutException e) {
                    System.out.println("Request to link "
                            +Crawler.URL_LIST.get(i)
                            +" by thread "
                            +Thread.currentThread().getName()
                            +" timed out. Trying to resend request...");
                    timeout=true;
                    
                } catch (HttpStatusException e) {
                    timeout=false;
                } catch (IOException ex) {
                    timeout=false;
                } catch (InterruptedException ex) {
                    timeout=false;
                }
            }
            if(allowed)
            {
                

                if(doc!=null)
                {
                    links=doc.getElementsByTag("a");
                    if(links!=null)//Crawler.URL_LIST.size()<MAX_DOCS&&
                    {
                        boolean GotAllPages =false;
                        System.out.println("Thread "+Thread.currentThread().getName()+
                            " with links at "+i+" = "+links.size());
                        
                            for (Element link : links)
                            {
                                synchronized(Crawler.LOCKER)
                                {
                                    if(!Crawler.URL_LIST.contains(link.attr("abs:href").toLowerCase())
                                            &&!link.attr("abs:href").contentEquals("")
                                            )//MaxDocs=Urlist initial size+ docsToAdd
                                    { 
                                        if(Crawler.URL_LIST.size()<MAX_DOCS)  
                                        {
                                            GotAllPages=true;
                                            Crawler.URL_LIST.add(link.attr("abs:href").toLowerCase());
                                          insertWebpageInDB(link.attr("abs:href").toLowerCase());
//                                            System.out.println(Thread.currentThread().getName()+" adding page"+
//                                                link.attr("abs:href"));    

                                        }
                                        else
                                        {
                                            GotAllPages=false;
                                        }
                                    }
//                                    else
//                                    {
//                                        System.out.println("search.engine.CrawlerThread.run()");
//                                    }
                                    insertOutlinkInDB(Crawler.URL_LIST.get(i),link.attr("abs:href").toLowerCase());
//                                    
                            }
                            
                        }
                            if(GotAllPages==true)
                            {
                                    DatabaseVisitToTrue(Crawler.URL_LIST.get(i));
                                    System.out.println("thread "+Thread.currentThread().getName()+
                                            "finished crawling of page "+Crawler.URL_LIST.get(i));
                                    PrintWriter outFile;
                try
                {
                    if(doc!=null)
                    {
                        outFile=new PrintWriter(
                                new BufferedWriter(
                                new FileWriter((initialNumWebpagesSaved+(i+1)+".html"))));


                        outFile.print(doc.toString());
                        updateFilenameOfWebpage(Crawler.URL_LIST.get(i),
                                (initialNumWebpagesSaved+(i+1)+".html"));
                        outFile.close();
                    }
                } catch (IOException ex) {
                    
                }
                            }
                            else
                            {
                                System.out.println("Thread "+Thread.currentThread().getName()+
                                "did not add all pages at "+i);
//                                return;
                            }
                    }
                    else
                    {
                      System.out.println("Thread "+Thread.currentThread().getName()+
                            "links =null "+i);
                    }
                }
                else
                {
                    System.out.println("Thread "+Thread.currentThread().getName()+
                                "doc = null "+i);
                }
                
                if(Crawler.URL_LIST.size()>=MAX_DOCS)//MaxDocs=Urlist initial size+ docsToAdd
                {
                    System.out.println("Thread "+Thread.currentThread().getName()+
                                " ended at counter= "+i);
                    return;
                }
            }
            else
                System.out.println("Thread "+Thread.currentThread().getName()+
                            " wasn't allowed to crawl link "
                        +Crawler.URL_LIST.get(i)
                        +" at index "+i+" as specified by Robot Exclusion Protocol");
        }
    }
    
    public int GetIndex(String ThreadName) throws InterruptedException
    {
            counter++;
            while(counter>=Crawler.URL_LIST.size())
            {
               Thread.sleep(1);
               if(Crawler.URL_LIST.size()>=MAX_DOCS )
               {
                   return counter;
               }
            }
            
            if(Crawler.URL_LIST.get(counter).contentEquals(""))
            {
                System.out.println(ThreadName+ " is waiting");
            }
            while(Crawler.URL_LIST.get(counter).contentEquals(""))
            {
                
            }
            return counter;
    }
    
    private void insertWebpageInDB(String url)
    {
        CallableStatement stmt;
        try {
            stmt = conn.prepareCall("{call dbo.InsertWebpage(?)}");
            stmt.setString("URL",url);
            stmt.execute();
        } catch (SQLException ex) {
            
        }
    }
    
    private void insertOutlinkInDB(String srcUrl, String dstUrl)
    {
        CallableStatement stmt;
        try {
            stmt = conn.prepareCall("{call dbo.InsertOutlink(?,?)}");
            stmt.setString("srcUrl",srcUrl);
            stmt.setString("dstUrl",dstUrl);
            stmt.execute();
        } catch (SQLException ex) {
            Logger.getLogger(CrawlerThread.class.getName()).log(Level.SEVERE, null,ex);
        }
    }

    private void DatabaseVisitToTrue(String url) {
        
        String updateTableSQL = "UPDATE Webpage SET Visited=1 where URL='"+url+"'";
        PreparedStatement preparedStatement;
        try {
            preparedStatement = conn.prepareStatement(updateTableSQL);
            preparedStatement.executeUpdate();
        } catch (SQLException ex) {
            
        }
        
    }

    private void updateFilenameOfWebpage(String url, String filename) {
        String updateTableSQL = "UPDATE Webpage SET Filename='"+filename+"' where URL='"+url+"'";
        PreparedStatement preparedStatement;
        try {
            preparedStatement = conn.prepareStatement(updateTableSQL);
            preparedStatement.executeUpdate();
        } catch (SQLException ex) {
            
        }
    }

    private int getNumWebpagesSaved() 
    {
        try {
            Statement stmt;
            stmt = conn.createStatement();
            ResultSet rs = stmt
                .executeQuery(
                "select count(WebpageID) from Webpage where [Filename] is not null");
            rs.next();
            return rs.getInt(1);
        } catch (SQLException ex) {
            return -1;
        }
    }
    
    boolean robotIsAllowed(String urlStr) throws MalformedURLException
    {
        URL url=new URL(urlStr);
        BufferedReader in=null;
        try {
            in = new BufferedReader(
                    new InputStreamReader(
                            new URL(url.getProtocol()
                                    +"://"+url.getHost()
                                    +"/robots.txt").openStream()));
        } catch (IOException ex) {
            
        }
        String line;
        try {
            if(in!=null)
                line=in.readLine();
            else
                return true;
            boolean check=false;
            String temp;
            while(line!=null)
            {
                line=line.trim();
                if(line.startsWith("User-agent:"))
                {
                    String[] lineArr=line.split("\\s+");
                    temp=lineArr[lineArr.length-1];
                    if(temp.equals("*"))
                        check=true;
                }
                else if(line.startsWith("Disallow:")&&check==true)
                {
                    String[] lineArr=line.split("\\s+");
                    temp=lineArr[lineArr.length-1];
                    if(urlStr.contains(temp))
                        return false;
                }
                else
                    check=false;
                line=in.readLine();
            }
            return true;
        } catch (IOException ex) {
            return true;
        }
    }
}