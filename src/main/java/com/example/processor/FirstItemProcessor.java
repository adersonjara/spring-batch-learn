package com.example.processor;

import com.example.model.StudentCsv;
import com.example.model.StudentJdbc;
import com.example.model.StudentJson;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

@Component
public class FirstItemProcessor implements ItemProcessor<StudentCsv, StudentJson> {
    @Override
    public StudentJson process(StudentCsv item) throws Exception {
        System.out.println("Inside Item Processor --");

        if (item.getId() == 5){
            System.out.println("error");
            throw new NullPointerException();
        }
        StudentJson studentJson = new StudentJson();
        studentJson.setId(item.getId());
        studentJson.setFirstName(item.getFirstName());
        studentJson.setLastName(item.getLastName());
        studentJson.setEmail(item.getEmail());
        return studentJson;
    }
}
