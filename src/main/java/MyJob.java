import com.splunk.*;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by markmo on 3/05/2016.
 */
public class MyJob implements Job {

    private Service service;
    private String query;
    private JobExportArgs exportArgs;
    private AtomicInteger i = new AtomicInteger(24);

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

        query = "search index=tr69 | table _raw";
        exportArgs = new JobExportArgs();
        exportArgs.setSearchMode(JobExportArgs.SearchMode.NORMAL);
    }

    public void execute(JobExecutionContext context) throws JobExecutionException {
        exportArgs.setEarliestTime("@d-" + i.get() + "h");
        exportArgs.setLatestTime("@d-" + i.decrementAndGet() + "h");
        exportArgs.setMaximumLines(0);
        exportArgs.setMaximumTime(0);

        try {
            InputStream exportSearch = service.export(query, exportArgs);
            MultiResultsReaderJson reader = new MultiResultsReaderJson(exportSearch);
            for (SearchResults results : reader) {
                for (Event event : results) {
                    System.out.println(event.get("_raw"));
                }
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
            throw new JobExecutionException(e.getMessage(), e);
        }
    }
}
