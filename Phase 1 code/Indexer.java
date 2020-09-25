/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package search.engine;
import java.sql.*;
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;
/**
 *
 * @author VAIO
 */
public class Indexer implements Runnable{
    
    private final Connection conn;
    private final Crawler C;
    public static final LinkedList<String> FILENAMES=new LinkedList<>();
    private static final int THREADS=2; //ramy , 4 threads kda 7lw awi , aktr mn kda hyb2a batee2 awi
    private Thread[] t;
    Indexer(Connection conn, Crawler C)
    {
        this.conn=conn;
        this.C=C;
    }
    
    @Override
    public void run()
    {
        try {
            index();
        } catch (InterruptedException ex) {
            Logger.getLogger(Indexer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public void index() throws InterruptedException
    {
        IndexerThread.setconn(conn);
        IndexerThread.setStemmer(new Stemmer());
        boolean emptyList=false;
        t=new Thread[THREADS];//ramy 5aleto public fo2 bs
        int ThreadCounter=0; //ramy , da sala7 error kan by7sal "array index out of range"
        while(!C.isFinished()||!emptyList)
        {
            retrieveFilenames();
            emptyList=FILENAMES.isEmpty();
            int offset=0;
            if(!emptyList)
            {
                Integer i=offset;
                while(i<FILENAMES.size())
                {
                    for(i=offset;i<THREADS+offset&&i<FILENAMES.size();i++)
                    {
                        t[ThreadCounter]=new Thread(new IndexerThread(FILENAMES.get(i),i,THREADS));//ramy,zawedt el params
                        t[ThreadCounter].setName(i.toString());
                        t[ThreadCounter].start();
                        
                        ThreadCounter++;//ramy
                    }
                    ThreadCounter=0;//ramy
                    for(i=offset;i<THREADS+offset&&i<FILENAMES.size();i++)
                    {
                        t[ThreadCounter].join();
                        ThreadCounter++;//ramy
                    }
                    ThreadCounter=0;//ramy
                    System.out.println("Indexed Finished Indexing ,will check if main list is updatednto index more");//ramy
                    break;//ramy 3ashan ybreak mn el inner while loop w ygaded el list
//                    offset+=THREADS;
//                    i=offset;
                }
            }
        }
    }
   
    
    private void retrieveFilenames() {
        FILENAMES.clear();
        try {
            PreparedStatement preparedStatement = conn.prepareStatement("select Filename "
                    + "from Webpage "
                    + "where Filename is not null and Indexed=0");
            ResultSet rs = preparedStatement.executeQuery();
            while(rs.next())
                FILENAMES.add(rs.getNString(1));
        } catch (SQLException ex) {
            Logger.getLogger(CrawlerThread.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
