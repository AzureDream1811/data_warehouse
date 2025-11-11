package etl;

import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.*;

public class DailyScheduler {
  private static ScheduledExecutorService exec;

  public static void scheduleAt(int hour, int minute, ZoneId zone, Runnable task) {
    exec = Executors.newSingleThreadScheduledExecutor();
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      if (exec != null)
        exec.shutdown();
    }));

    long delay = millisUntilNextRun(hour, minute, zone);
    long period = TimeUnit.DAYS.toMillis(1);
    exec.scheduleAtFixedRate(task, delay, period, TimeUnit.MILLISECONDS);
    System.out.println("📅 Daily ETL at %02d:%02d %s".formatted(hour, minute, zone));
  }

  private static long millisUntilNextRun(int hour, int minute, ZoneId zone) {
    ZonedDateTime now = ZonedDateTime.now(zone);
    ZonedDateTime next = now.withHour(hour).withMinute(minute).withSecond(0).withNano(0);
    if (!next.isAfter(now))
      next = next.plusDays(1);
    return ChronoUnit.MILLIS.between(now, next);
  }
}
