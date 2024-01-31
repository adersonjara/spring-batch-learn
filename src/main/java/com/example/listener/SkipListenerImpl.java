package com.example.listener;

import com.example.model.StudentCsv;
import com.example.model.StudentJson;
import org.springframework.batch.core.SkipListener;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileWriter;
import java.util.Date;

@Component
public class SkipListenerImpl implements SkipListener<StudentCsv, StudentJson> {
    @Override
    public void onSkipInRead(Throwable th) {
        if (th instanceof FlatFileParseException){
            createFile("C:\\java\\workspace\\demo\\Chunk Job\\First Chunk Step\\reader\\SkipInRead.txt",
                    ((FlatFileParseException) th).getInput());
        }
    }

    @Override
    public void onSkipInWrite(StudentJson item, Throwable t) {
        createFile("C:\\java\\workspace\\demo\\Chunk Job\\First Chunk Step\\writer\\SkipInWrite.txt",
                item.toString());
    }

    @Override
    public void onSkipInProcess(StudentCsv item, Throwable t) {
        createFile("C:\\java\\workspace\\demo\\Chunk Job\\First Chunk Step\\processer\\SkipInProcess.txt",
                item.toString());
    }

    public void createFile(String filePath, String data){
        try(FileWriter fileWriter = new FileWriter(new File(filePath), true)){
            fileWriter.write(data + "," + new Date() +"\n");
        }catch (Exception e){

        }
    }
}
