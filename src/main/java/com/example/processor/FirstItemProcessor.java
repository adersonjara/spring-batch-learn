package com.example.processor;

import com.example.model.StudentCsv;
import com.example.model.StudentJdbc;
import com.example.model.StudentJson;
import com.example.postgresql.entity.Student;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

@Component
public class FirstItemProcessor implements ItemProcessor<Student, com.example.mysql.entity.Student> {
    @Override
    public com.example.mysql.entity.Student process(Student item) throws Exception {

        System.out.println(" * "+item.toString());

        com.example.mysql.entity.Student student =
                new com.example.mysql.entity.Student();

        student.setId(item.getId());
        student.setFirstName(item.getFirstName());
        student.setLastName(item.getLastName());
        student.setEmail(item.getEmail());
        student.setDeptId(item.getDeptId());
        student.setIsActive(item.getIsActive() != null ?
                Boolean.valueOf(item.getIsActive()) : false);

        return student;
    }
}
