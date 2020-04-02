package org.bonitasoft.truckmilk.toolbox;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugInParameter;
import org.bonitasoft.truckmilk.job.MilkJobExecution;
import org.bonitasoft.truckmilk.plugin.MilkCancelCases;

/* ******************************************************************************** */
/*                                                                                  */
/* MilkUnassignTasks */
/*                                                                                  */
/* Unassign tasks */
/*                                                                                  */
/* ******************************************************************************** */


public class CSVOperation {
    static MilkLog logger = MilkLog.getLogger(MilkCancelCases.class.getName());


    /* ******************************************************************************** */
    /*                                                                                  */
    /* Read a CSV File from a InputParameter */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */


    private BufferedReader reader;
    private long lineNumber;
    private String[] header;
    private String separatorCSV;
    private long nbLines;
    
    /**
     * load the document and return the number of line in the CSV.
     * @param jobExecution
     * @param inputParameter
     * @return
     * @throws IOException 
     */
    public long loadCsvDocument(MilkJobExecution jobExecution, PlugInParameter inputParameter, String separatorCSV) throws IOException {
        ByteArrayOutputStream inputCSV = new ByteArrayOutputStream();
        jobExecution.getStreamParameter(inputParameter, inputCSV);
        if (inputCSV.size() == 0) {
            return 0;
        }
        this.separatorCSV = separatorCSV;
        nbLines = nbLinesInCsv( inputCSV );
        reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(inputCSV.toByteArray())));
        String line = reader.readLine();
        // read the header
        header = line == null ? new String[0] : line.split(separatorCSV);
        lineNumber = 1;
        return nbLines;
        
    }

    public long getCount() {
        return nbLines;
    }
    public String[] getHeader() {
        return header;
    }
    public Map<String, String> getNextRecord() throws IOException {
        String line = reader.readLine();
        if (line==null)
            return null;
        lineNumber++;
        return getMap(header, line, separatorCSV);

    }
    
    public long getCurrentLineNumber() {
        return lineNumber;
    }
    
    private long nbLinesInCsv(ByteArrayOutputStream inputCSV) {
        try {
            BufferedReader readerLine = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(inputCSV.toByteArray())));
            long nbLine = 0;
            while (readerLine.readLine() != null) {
                nbLine++;
            }
            return nbLine-1;
        } catch (Exception e) {
            return 0;
        }

    }
    
    private Map<String, String> getMap(String[] header, String line, String separatorCSV) {
        Map<String, String> record = new HashMap<>();
        // don't use a StringTokenizer : if the line contains ;; for an empty information, StringTokenizer will merge the two separator

        List<String> listString = getStringTokenizerPlus(line, separatorCSV);
        for (int i = 0; i < header.length; i++) {
            record.put(header[i], i < listString.size() ? listString.get(i) : null);
        }
        return record;
    }
    
    /**
     * @param line
     * @param charSeparator
     * @return
     */
    private List<String> getStringTokenizerPlus(String line, final String charSeparator) {
        final List<String> list = new ArrayList<>();
        int index = 0;
        if (line == null || line.length() == 0) {
            return list;
        }
        // now remove all empty string at the end of the list (keep the minimal)
        // then if string is "hello;this;;is;the;word;;;;"
        // line is reduce to "hello;this;;is;the;word"
        // nota : if the line is
        // then if string is "hello;this;;is;the;word;; ;;"
        // then "hello;this;;is;the;word;; "
        while (line.endsWith(";"))
            line = line.substring(0, line.length() - 1);
        while (index != -1) {
            final int nextPost = line.indexOf(charSeparator, index);
            if (nextPost == -1) {
                list.add(line.substring(index));
                break;
            } else {
                list.add(line.substring(index, nextPost));
            }
            index = nextPost + 1;
        }

        return list;
    }
    
    
    /**
     * For debug
     */
    public static void saveFile( String fileName, InputStream instream) 
    {
        try {
        instream.reset();
        FileOutputStream fileWrite = new FileOutputStream(fileName);
        
        byte[] buffer = new byte[10000];
        while (true) {
            int bytesRead;
            bytesRead = instream.read(buffer);
            if (bytesRead == -1)
                break;
            fileWrite.write(buffer, 0, bytesRead);
        }
        fileWrite.flush();
        fileWrite.close();
        instream.reset();
        }catch(Exception e) {            
            logger.severe("During SaveFile "+e.getLocalizedMessage());
        }
        

    }

}
