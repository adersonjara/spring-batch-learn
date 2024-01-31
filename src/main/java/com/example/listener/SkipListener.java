package com.example.listener;

import org.springframework.batch.core.annotation.OnSkipInRead;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileWriter;
import java.util.Date;

@Component
public class SkipListener {

    @OnSkipInRead
    public void skipInRead(Throwable th){
        if (th instanceof FlatFileParseException){
            createFile("C:\\java\\workspace\\demo\\Chunk Job\\First Chunk Step\\reader\\SkipInRead.txt",
                    ((FlatFileParseException) th).getInput());
        }
    }

    public void createFile(String filePath, String data){
        try(FileWriter fileWriter = new FileWriter(new File(filePath), true)){
            fileWriter.write(data + "," + new Date() +"\n");
        }catch (Exception e){

        }
    }
}
