import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class Main {
   static final int TimeWindow = 60; // in minutes

    public static void main(String[] args) {
       String tradeTime = "2019010211459870";
       String ans=createTimeKey(tradeTime);
       System.out.println(ans);
    }
    private static String createTimeKey(String tradeTime) { // convert input time to the start of a time window
        // trade time format: yyyyMMddHHmmSSsss
        // extract y, M, d, H, m, S, s
        int year = Integer.parseInt(tradeTime.substring(0, 4));
        int month = Integer.parseInt(tradeTime.substring(4, 6));
        int day = Integer.parseInt(tradeTime.substring(6, 8));
        int hour = Integer.parseInt(tradeTime.substring(8, 10));
        int minute = Integer.parseInt(tradeTime.substring(10, 12));

        // set base time as 9:30
        int baseHour;
        int baseMinute;
        if (hour == 15) {
            hour = 14;
            minute = 59;
        }
        if (hour < 9 || (hour == 9 && minute < 30)) {
            return null; // 早于9:30返回null
        }

        if (hour <= 12) {
            baseHour = 9;
            baseMinute = 30;
        } else {
            baseHour = 13;
            baseMinute = 0;
        }

        // convert input time to total minutes
        int inputTotalMinutes = (hour * 60 + minute);
        int baseTotalMinutes = (baseHour * 60 + baseMinute);

        // calculate time slot index
        int timeSlotIndex = (inputTotalMinutes - baseTotalMinutes) / (TimeWindow / 60);

        // calculate current and next time slots
        int startMinutes = baseTotalMinutes + timeSlotIndex * (TimeWindow / 60);
        int endMinutes = startMinutes + (TimeWindow / 60);

        // converting back to hours and minutes
        int finalStartHour = startMinutes / 60;
        int finalStartMinute = startMinutes % 60;

        int finalEndHour = endMinutes / 60;
        int finalEndMinute = endMinutes % 60;

        if(finalEndHour >= 15){
            finalEndHour = 15;
            finalEndMinute = 0;
        }
        // output in the desired format
        return String.format("%d%02d%02d%02d%02d00000 to %d%02d%02d%02d%02d00000",
                year, month, day, finalStartHour, finalStartMinute,
                year, month, day, finalEndHour, finalEndMinute);
    }
}
