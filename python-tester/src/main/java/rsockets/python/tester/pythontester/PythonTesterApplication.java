package rsockets.python.tester.pythontester;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class PythonTesterApplication {

	public static void main(String[] args) {
		try {
			SpringApplication.run(PythonTesterApplication.class, args);
			
		} catch (Exception e) {
			System.out.println(e);
		}
	}

}
