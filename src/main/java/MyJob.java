import com.splunk.*;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;

/**
 * Created by markmo on 3/05/2016.
 */
public class MyJob implements Job {

    private Service service;
    private String query;
    private JobArgs jobArgs;

    public MyJob() {
        Properties conf = new Properties();
        try {
            InputStream input = getClass().getClassLoader().getResourceAsStream("config.properties");
            conf.load(input);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Cannot load config: " + e.getMessage(), e);
        }

        ServiceArgs loginArgs = new ServiceArgs();
        loginArgs.setUsername(conf.getProperty("username"));
        loginArgs.setPassword(conf.getProperty("password"));
        loginArgs.setHost(conf.getProperty("host"));
        loginArgs.setPort(Integer.parseInt(conf.getProperty("port")));

        HttpService.setSslSecurityProtocol(SSLSecurityProtocol.TLSv1_2);

        service = Service.connect(loginArgs);

        query = "search *";
        jobArgs = new JobArgs();
        jobArgs.setExecutionMode(JobArgs.ExecutionMode.NORMAL);
    }

    public void execute(JobExecutionContext context) throws JobExecutionException {

        com.splunk.Job job = service.getJobs().create(query, jobArgs);

        while (!job.isDone()) {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        JobResultsArgs resultsArgs = new JobResultsArgs();
        resultsArgs.setOutputMode(JobResultsArgs.OutputMode.JSON);

        try {
            InputStream results = job.getResults(resultsArgs);
            ResultsReaderJson reader = new ResultsReaderJson(results);
            HashMap<String, String> event;
            while ((event = reader.getNextEvent()) != null) {
                System.out.println(event.get("_raw"));
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
            throw new JobExecutionException(e.getMessage(), e);
        }
    }
}
