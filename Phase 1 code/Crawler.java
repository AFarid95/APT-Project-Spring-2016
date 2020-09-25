/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package search.engine;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;
/**
 *
 * @author VAIO
 */
public class Crawler implements Runnable{

    public final static LinkedList<String> URL_LIST=new LinkedList<>();
    public final static Integer LOCKER=0;
    private final Connection conn;
    public static Thread[] ThreadArray;
    private final int DocsToAdd=5000;
    private boolean finished=false;

    Crawler(Connection conn)
    {
        this.conn=conn;
       
        insertSeedsInDB();
        retrieveNonVisitedLinks();
//        
        CrawlerThread.MAX_DOCS=DocsToAdd+URL_LIST.size();
    }
    
    @Override
    public void run()
    {
        try {
            crawl();
        } catch (InterruptedException ex) {
            Logger.getLogger(Crawler.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    private void insertSeedsInDB() {
        try {
            CallableStatement stmt;
            stmt = conn.prepareCall("{call dbo.InsertSeeds}");
            stmt.execute();
        } catch (SQLException ex) {
            Logger.getLogger(SearchEngine.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void retrieveNonVisitedLinks() {
        try {
            Statement stmt;
            stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select URL from Webpage where Visited=0");
            while(rs.next())
                URL_LIST.add(rs.getNString(1));
        } catch (SQLException ex) {
            Logger.getLogger(SearchEngine.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public void crawl() throws InterruptedException
    {
        System.out.println("enter number of threads");
        Scanner S=new Scanner(System.in) ;
        int size =S.nextInt();
        ThreadArray= new Thread[size];
        for (int i = 0 ; i< size; i++ )
         {
          Integer x=i;
          String name=x.toString();
          ThreadArray[i]=new Thread(new CrawlerThread(conn));
          ThreadArray[i].setName(name);
         }
         long time1=System.currentTimeMillis();
         CrawlerThread.counter=-1;
         
        for (int i = 0 ; i< size; i++ )
         {
            ThreadArray[i].start();
         }
        for (int i = 0 ; i< size; i++ )
         {
            ThreadArray[i].join();
         }

        long time2=System.currentTimeMillis(); 
        System.out.println("Threads finished crawling in "+ (time2-time1)+" ms");
        System.out.println("Added "+DocsToAdd+" Documents ,List new size ="+URL_LIST.size());
        finished=true;
    }
    
    public boolean isFinished()
    {
        return finished;
    }
}
