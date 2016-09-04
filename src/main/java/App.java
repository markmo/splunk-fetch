import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.Trigger;
import org.quartz.impl.StdSchedulerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

import static org.quartz.JobBuilder.newJob;
import static org.quartz.SimpleScheduleBuilder.simpleSchedule;
import static org.quartz.TriggerBuilder.newTrigger;

/**
 * Created by markmo on 3/05/2016.
 */
public class App {

    /**
     * Usage:
     * App "2016-05-04 1:00" 10 10
     *
     * @param args String[] StartTime IntervalInMinutes RepeatCount
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        if (args == null || args.length != 3) {
            throw new RuntimeException("Invalid args: StartTime IntervalInMinutes RepeatCount");
        }

        Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();

        JobDetail job = newJob(MyJob.class)
                .withIdentity("job1", "group1")
                .build();

        // datetime in 24hr format, e.g. 2016-05-04 21:00
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd k:mm");
        Date time = formatter.parse(args[0]);

        Trigger trigger = newTrigger()
                .withIdentity("trigger1", "group1")
                .startAt(time)
                .withSchedule(simpleSchedule()
                        .withIntervalInMinutes(Integer.parseInt(args[1]))
                        .withRepeatCount(Integer.parseInt(args[2])))
                .build();

        scheduler.scheduleJob(job, trigger);

        scheduler.start();
    }
}
