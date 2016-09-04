import com.splunk.*;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by markmo on 3/05/2016.
 */
public class MyJob implements Job {

    private Service service;
    private String query;
    private JobExportArgs exportArgs;
    private static AtomicInteger i = new AtomicInteger(24);

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

        HttpService.setSslSecurityProtocol(SSLSecurityProtocol.TLSv1);

        service = Service.connect(loginArgs);

        query = "search index=tr69 | table _raw";
        exportArgs = new JobExportArgs();
        exportArgs.setSearchMode(JobExportArgs.SearchMode.NORMAL);
    }

    public void execute(JobExecutionContext context) throws JobExecutionException {
        String earliestTime = i.get() + "h";
        exportArgs.setEarliestTime("@d-" + earliestTime);
        exportArgs.setLatestTime("@d-" + i.decrementAndGet() + "h");
        exportArgs.setMaximumLines(0);
        exportArgs.setMaximumTime(0);
        exportArgs.setOutputMode(JobExportArgs.OutputMode.JSON);

        BufferedWriter out = null;
        try {
            SimpleDateFormat formatter1 = new SimpleDateFormat("yyyyMMdd_HHmm");
            SimpleDateFormat formatter2 = new SimpleDateFormat("yyyyMMdd");
            Calendar cal = Calendar.getInstance();
            String now = formatter1.format(cal.getTime());
            cal.add(Calendar.DAY_OF_MONTH, -1);
            String startTime = formatter2.format(cal.getTime()) + "_" + earliestTime;
            File targetFile = new File("/data/tdc/cxp/prd/landing/datain/tr069/raw_" + startTime + "_" + now + ".raw");
            if (!targetFile.exists()) {
                targetFile.createNewFile();
            }
            out = new BufferedWriter(new FileWriter(targetFile));
            InputStream exportSearch = service.export(query, exportArgs);
            MultiResultsReaderJson reader = new MultiResultsReaderJson(exportSearch);
            String content = null;
            for (SearchResults results : reader) {
                for (Event event : results) {
//                    System.out.println(event.get("_raw"));
                    content = event.get("_raw");
                    out.write(content);
                }
            }
            out.flush();
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
            throw new JobExecutionException(e.getMessage(), e);
        } finally {
            try {
                if (out != null) {
                    out.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
