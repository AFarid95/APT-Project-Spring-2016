package SearchEnginePhase2;
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import javax.servlet.*;
import javax.servlet.http.*;
import static java.lang.Math.ceil;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.BitSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jsoup.*;
import org.jsoup.nodes.Document;
/**
 *
 * @author VAIO
 */
public class Servlet extends HttpServlet {
    
    private static final String DB_URL = "jdbc:sqlserver://localhost:1433;databaseName=SearchEngineIndex";
    private static final String USER = "sa";
    private static final String PASS = "p@ssword13";
    private static Connection conn = null;
    private static final int MAX_WEBPAGES=100;
    private static final int WEBPAGES_PER_PAGE=10;
    
    @Override
    public void doPost(HttpServletRequest request,
                     HttpServletResponse response)
      throws ServletException, IOException 
    {
        try{
            PrintWriter out;
            response.setContentType("text/html");
            out = response.getWriter();
            if(!connectToIndex())
            {
                out.print("Connection failed");
                return;
            }
            
            String query = request.getParameter("query");
            query=query.trim();
            boolean isPhrase=checkIfPhrase(query);
            if(isPhrase)
                saveQuery(query.substring(0,query.length()-2));
            else
                saveQuery(query);
            
            QueryProcessor qp = new QueryProcessor(conn);
            int[][] webpagesArray=qp.process(query,isPhrase);
//            int[] webpages=new int[webpagesArray.length];
//            for(int i=0;i<webpagesArray.length;i++)
//                webpages[i]=webpagesArray[i][0];
            Ranker r = new Ranker(conn);
            int[] webpages=r.rank(webpagesArray,query,isPhrase);

            displayResults(query,isPhrase,webpages,request,response);

            closeConnection();
            }catch(SQLException ex){
                Logger.getLogger(Servlet.class.getName()).log(Level.SEVERE, null,ex);
            }
    }
    
    public static void main(String[] args)
    {
//        PrintWriter out=null;
//        response.setContentType("text/html");
//            out = response.getWriter();
        if(!connectToIndex())
        {
//            out.print("Connection failed");
            return;
        }
//
        String query = "the"; //will be changed
        query=query.trim();
        boolean isPhrase=checkIfPhrase(query);
        //enter the query in the database

        QueryProcessor qp = new QueryProcessor(conn);
        int[][] webpagesArray=qp.process(query,isPhrase);
//        int[] webpages=new int[webpagesArray.length];
//        for(int i=0;i<webpagesArray.length;i++)
//            webpages[i]=webpagesArray[i][0];
        Ranker r = new Ranker(conn);
        int[] webpages=r.rank(webpagesArray,query,isPhrase);
//        //output the webpages to the user
//
//        try{
//            displayResults(query,isPhrase,webpages,response);
//        }catch(Exception ex){
//            response.getWriter().print(ex);
//        }

        closeConnection();
    }
    
    private static boolean connectToIndex()
    {
        try{
            
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");

            System.out.println("Connecting to database...");
            conn = DriverManager.getConnection(DB_URL,USER,PASS);
            return true;
            
         }catch(SQLException se){
             System.out.println("Connection to index failed: Error in JDBC");
             return false;
         }catch(ClassNotFoundException e){
             System.out.println("Connection to index failed: Error in Class.forName");
             return false;
         }
    }
    
    private static void closeConnection()
    {
        try{
         if(conn!=null)
            conn.close();
        }catch(SQLException se){
            System.out.println("Closing the connection failed");
        }
    }

    private void displayResults(String query,
            boolean isPhrase, 
            int[] webpages,
            HttpServletRequest request,
            HttpServletResponse response) throws IOException, ServletException {
        int numOutputPages=(int)ceil((double)webpages.length/(double)WEBPAGES_PER_PAGE);
        PrintWriter outFile;
        Document webpageDoc;
        String filename;
        String webpageTitle,webpageUrl,webpageText;
        String[] tagsInParagraph={"h1","h2","h3","h4","h5","h6","p","table","li","a"};
        for(int i=0;i<numOutputPages;i++) //for each output page
        {
            outFile=getFileWriter("Output"+(i+1)+".html");
            outputBeginning(outFile,query,isPhrase,i+1,numOutputPages);
            for(int j=i*WEBPAGES_PER_PAGE;
                    j<(i+1)*WEBPAGES_PER_PAGE&&j<webpages.length;
                    j++) //for each webpage in results
            {
                filename=retrieveFilenameFromDB(webpages[j]);
                webpageDoc=Jsoup.parse(getFile(filename),"UTF-8");
                webpageTitle=webpageDoc.getElementsByTag("title").text();
                webpageUrl=retrieveUrlFromDB(webpages[j]);
                webpageText="";
                for(String paragraphTag:tagsInParagraph)
                {
                    webpageText+=webpageDoc.getElementsByTag(paragraphTag).text();
                    webpageText+=" ";
                }
                outFile.print("<div class=\"webpageTitle\">");
                outFile.print("<a href=\""+webpageUrl+"\">"+webpageTitle+"</a>");
                outFile.print("</div><br>");
                outFile.print("<div class=\"webpageUrl\">");
                outFile.print(webpageUrl);
                outFile.print("</div><br>");
                outFile.print("<div class=\"webpageText\" style=\"line-height:100%;\">");
                outFile.print(getSampleText(webpageDoc,query,isPhrase));
                outFile.print("</div><br><br><br><br>");
            }
            outFile.print("<br><br>"
                    + "<p align=\"center\" style=\"font-family:Calibri;font-size:12pt;\"><b>"
                    + "Select Page</b></p><br>");
            outFile.print("<div class=\"pageNumber\"  align=\"center\">");
            for(int j=0;true;j++)
            {
                if(i==j)
                    outFile.print(j+1);
                else
                    outFile.print("<a "
                            + "class=\"pageNumber\" "
                            + "href=\"/Search-Engine-HTML-Files/"
                            + "Output"+(j+1)+".html\">"
                            + (j+1)+"</a>");
                if(j+1==numOutputPages)
                    break;
                outFile.print(" ");
            }
            outFile.print("</div>");
            outputEnd(outFile);
            outFile.close();
        }
        RequestDispatcher view = request.getRequestDispatcher("/Search-Engine-HTML-Files/Output1.html");
        view.forward(request, response);
    }
    
    private BufferedReader getFileReader(String filename)
    {
        ServletContext context = getServletContext();
        InputStream is = context.getResourceAsStream("/Search-Engine-HTML-Files/"+filename);
        if (is != null) {
            InputStreamReader isr = new InputStreamReader(is);
            return new BufferedReader(isr);
        }
        return null;
    }
    
    private PrintWriter getFileWriter(String filename) throws IOException
    {
        ServletContext context = getServletContext();
        File outFile = new File(context.getRealPath("/Search-Engine-HTML-Files/"+filename));
        return new PrintWriter(new BufferedWriter(new FileWriter(outFile)));
    }

    private File getFile(String filename) {
        ServletContext context = getServletContext();
        return new File(context.getRealPath("/Search-Engine-HTML-Files/"+filename));
    }
    
    private void outputBeginning(PrintWriter outFile,
            String query,
            boolean isPhrase,
            int currentPage,
            int totalPages)
    {
        String str;
        if(isPhrase)
            str="Showing search results for "+query+" (phrase search)";
        else
            str="Showing search results for \""+query+"\" (non-phrase search)";
        str+="<br><br><br>Page "+currentPage+" of "+totalPages;
        outFile.print("<!DOCTYPE html>\n" +
                    "<html>\n" +
                    "<head>\n" +
                    "<style>\n"+
                    "body {\n" +
                    "   background: url(OutputBackground.jpg) repeat;\n" +
                    "	background-size: 100%;\n" +
                    "}\n" +
                    ".wholePageTitle {\n" +
                    "	font-family:Calibri;\n" +
                    "	font-size:20pt;\n" +
                    "	font-weight:bold;\n" +
                    "	color:black;\n" +
                    "	word-wrap:break-word;\n" +
                    "	width:300px;\n" +
                    "}\n" +
                    ".webpageTitle {\n" +
                    "	font-family:Calibri;\n" +
                    "	font-size:18pt;\n" +
                    "	font-weight:bold;\n" +
                    "	text-decoration:underline;\n" +
                    "	color:#0000CC;\n" +
                    "}\n" +
                    ".webpageUrl {\n" +
                    "	font-family:Calibri;\n" +
                    "	font-size:14pt;\n" +
                    "	color:#009900;\n" +
                    "}\n" +
                    ".webpageText {\n" +
                    "	font-family:Calibri;\n" +
                    "	font-size:12pt;\n" +
                    "  word-wrap:break-word;\n" +
                    "	width:500px;\n" +
                    "}\n" +
                    ".pageNumber {\n" +
                    "	font-family:Calibri;\n" +
                    "	font-size:12pt;\n" +
                    "	color:black;\n" +
                    "	word-spacing:10px;\n" +
                    "}\n" +
                    "a.pageNumber {\n" +
                    "	font-family:Calibri;\n" +
                    "	font-size:12pt;\n" +
                    "	color:#0000CC;\n" +
                    "	text-decoration:underline;\n" +
                    "}\n" +
                    "</style>\n" +
                    "</head>\n" +
                    "<body style=\"line-height:75%;\">" +
                    "<img src=\"Logo.jpg\"></img><br><br><br>" +
                    "<div class=\"wholePageTitle\" "
                + "style=\"white-space:nowrap;\">"
                +str+"</div><br><br><br><br>");
    }
    
    private void outputEnd(PrintWriter outFile)
    {
        outFile.print("</body>\n" +
                    "</html>");
    }

    private static boolean checkIfPhrase(String query) {
        return query.startsWith("\"")&&query.endsWith("\"");
    }
    
    public static int getMaxWebpages()
    {
        return MAX_WEBPAGES;
    }
    
    public static int getWebpagesPerPage()
    {
        return WEBPAGES_PER_PAGE;
    }

    private String retrieveFilenameFromDB(int webpage) {
        try {
            PreparedStatement preparedStatement = conn.prepareStatement("select Filename "
                    + "from Webpage "
                    + "where WebpageID="+webpage);
            ResultSet rs = preparedStatement.executeQuery();
            if(rs.next())
                return rs.getNString(1);
            return "";
        } catch (SQLException ex) {
            Logger.getLogger(Servlet.class.getName()).log(Level.SEVERE, null, ex);
            return ex.toString()+"<br>";
        }
    }

    private String retrieveUrlFromDB(int webpage) {
        try {
            PreparedStatement preparedStatement = conn.prepareStatement("select URL "
                    + "from Webpage "
                    + "where WebpageID="+webpage);
            ResultSet rs = preparedStatement.executeQuery();
            if(rs.next())
                return rs.getNString(1);
            return "";
        } catch (SQLException ex) {
            Logger.getLogger(Servlet.class.getName()).log(Level.SEVERE, null, ex);
            return ex.toString()+"<br>";
        }
    }

    private void saveQuery(String query) throws SQLException {
        CallableStatement insertQuery
                        = conn.prepareCall("{call dbo.InsertQuery(?)}");
        insertQuery.setNString(1,query);
        insertQuery.execute();
    }

    private String getSampleText(Document webpageDoc, String query, boolean phrase) {
        String[] sentences=webpageDoc
                .getElementsByTag("body")
                .text()
                .split("\\.|\\?|!");
        String returned="";
        if(!phrase)
        {
            String[] wordsInQuery=query.split("\\W+");
            BitSet wordFound=new BitSet(wordsInQuery.length);
            BitSet sentenceAdded=new BitSet(sentences.length);
            for(int i=0;i<sentences.length;i++)
            {
                if(sentences[i].length()<500) //normal sentence
                {
                    for(int j=0;j<wordsInQuery.length;j++)
                    {
                        if(sentences[i].contains(wordsInQuery[j]))
                        {
                            if(!sentenceAdded.get(i))
                            {
                                returned+=sentences[i]+" ... ";
                                sentenceAdded.set(i);
                            }
                            wordFound.set(j);
                        }
                        if(wordFound.cardinality()==wordsInQuery.length)
                            return returned;
                    }
                }
                else //very long sentence
                {
                    for(int j=0;j<wordsInQuery.length;j++)
                    {
                        if(sentences[i].contains(wordsInQuery[j]))
                        {
                            if(!sentenceAdded.get(i))
                            {
                                String addedPart;
                                int indexOfWord,start,end,spaceCounter;
                                indexOfWord=sentences[i].indexOf(wordsInQuery[j]);
                                start=indexOfWord;
                                end=indexOfWord;
                                spaceCounter=0;
                                while(spaceCounter<6&&start>-1)
                                {
                                    start--;
                                    if(sentences[i].charAt(start)==' ')
                                        spaceCounter++;
                                }
                                spaceCounter=0;
                                while(spaceCounter<6&&end<sentences[i].length())
                                {
                                    end++;
                                    if(sentences[i].charAt(end)==' ')
                                        spaceCounter++;
                                }
                                addedPart=sentences[i].substring(start+1,end);
                                returned+=addedPart+" ... ";
                                sentenceAdded.set(i);
                            }
                            wordFound.set(j);
                        }
                        if(wordFound.cardinality()==wordsInQuery.length)
                            return returned;
                    }
                }
                if(returned.length()>=500)
                    return returned;
            }
            if(wordFound.cardinality()==0) //if body doesn't contain any word in the query
                return sentences[0].substring(0,501);
            return returned;
        }
        else
        {
            for(int i=0;i<sentences.length;i++)
            {
                if(sentences[i].length()<500) //normal sentence
                {
                    if(sentences[i].contains(query))
                    {
                        returned+=sentences[i]+" ... ";
                        return returned;
                    }
                }
                else //very long sentence
                {
                    if(sentences[i].contains(query))
                    {
                            String addedPart;
                            int indexOfWord,start,end,spaceCounter;
                            indexOfWord=sentences[i].indexOf(query);
                            start=indexOfWord;
                            end=indexOfWord;
                            spaceCounter=0;
                            while(spaceCounter<6&&start>-1)
                            {
                                start--;
                                if(sentences[i].charAt(start)==' ')
                                    spaceCounter++;
                            }
                            spaceCounter=0;
                            while(spaceCounter<query.length()+6
                                    &&end<sentences[i].length())
                            {
                                end++;
                                if(sentences[i].charAt(start)==' ')
                                    spaceCounter++;
                            }
                            addedPart=sentences[i].substring(start+1,end);
                            returned+=addedPart+" ... ";
                            return returned;
                    }
                }
                if(returned.length()>=500)
                    return returned;
            }
            return sentences[0].substring(0,501); //if body doesn't contain the phrase
        }
    }
}
