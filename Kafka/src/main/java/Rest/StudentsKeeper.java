package Rest;

import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/students")
public class StudentsKeeper {
 //XXX: we have several threads...
 private static List<String> students = new ArrayList<>();
 
 @POST
 public void addStudent(String student) {
  System.out.println("Called post addStudent with parameter: " + student);
  System.out.println("Thread = " + Thread.currentThread().getName());
  students.add(student);
 }

 @GET
 @Produces(MediaType.APPLICATION_JSON)
 public List<String> getAllStudents() {
  System.out.println("Called getAllStudents");
  System.out.println("Thread = " + Thread.currentThread().getName());
  return students;
 }

 
 /* 
  * 
  * Postman request POST: information must be passed on body in order to work
  * 
  * */
 @Path("xpto")
 @GET
 @Produces(MediaType.APPLICATION_JSON)
 public String getAllStudents2() {
  System.out.println("Called getAllStudents 2");
  System.out.println("Thread = " + Thread.currentThread().getName());
  return "test";
 }

}