package com.example.listener;

import com.example.model.StudentCsv;
import com.example.model.StudentJson;
import org.springframework.batch.core.annotation.OnSkipInProcess;
import org.springframework.batch.core.annotation.OnSkipInRead;
import org.springframework.batch.core.annotation.OnSkipInWrite;
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

    @OnSkipInProcess
    public void skipInProcess(StudentCsv studentCsv, Throwable th){

            createFile("C:\\java\\workspace\\demo\\Chunk Job\\First Chunk Step\\processer\\SkipInProcess.txt",
                    studentCsv.toString());

    }

    @OnSkipInWrite
    public void skipInWrite(StudentJson studentJson, Throwable th){

        createFile("C:\\java\\workspace\\demo\\Chunk Job\\First Chunk Step\\writer\\SkipInWrite.txt",
                studentJson.toString());

    }

    public void createFile(String filePath, String data){
        try(FileWriter fileWriter = new FileWriter(new File(filePath), true)){
            fileWriter.write(data + "," + new Date() +"\n");
        }catch (Exception e){

        }
    }
}
