package com.znwhahaha;

import java.io.FileWriter;
import java.io.IOException;

public class WriteRunLog {
    
    public static void writeToFiles(String dataStr) throws IOException{

        String logPath = "/root/Hotspots/logs/log.xml";

        FileWriter writer = new FileWriter(logPath, true);
        writer.write(dataStr + "\n");
        writer.close();

    }
}
