public class Main {
    public static void main(String[] args) {
        System.out.println(createTimeKey("201901021300200000",300));
    }
    private static String createTimeKey(String tradeTime, int TimeWindow) {
        // trade time format: yyyyMMddHHmmSSsss
        // extract y, M, d, H, m, S, s
        int year = Integer.parseInt(tradeTime.substring(0, 4));
        int month = Integer.parseInt(tradeTime.substring(4, 6));
        int day = Integer.parseInt(tradeTime.substring(6, 8));
        int hour = Integer.parseInt(tradeTime.substring(8, 10));
        int minute = Integer.parseInt(tradeTime.substring(10, 12));

        // 基准时间设置为 9:30
        int baseHour = 9;
        int baseMinute = 30;

        // 将输入的时间转换为绝对分钟数
        int inputTotalMinutes = (hour * 60 + minute);

        // 将 9:30 转换为绝对分钟数
        int baseTotalMinutes = (baseHour * 60 + baseMinute);

        // 计算时间间隔
        int timeSlotIndex;
        if (inputTotalMinutes < baseTotalMinutes) {
            // 输入时间在 9:30 之前，时间间隔的计算方式
            timeSlotIndex = (inputTotalMinutes - baseTotalMinutes - (TimeWindow / 60)) / (TimeWindow / 60);; // -10 表示时间需要向前推进到所在的时间段
        } else {
            timeSlotIndex = (inputTotalMinutes - baseTotalMinutes) / (TimeWindow / 60);; // 正常情况下
        }

        // 计算新的分钟数，返回的时间点为下一个时间段的起始时间
        int newBaseMinute = baseMinute + timeSlotIndex * (TimeWindow / 60);;

        // 如果 newBaseMinute 超过 59 分钟，需要进位到小时
        int finalHour = baseHour + newBaseMinute / 60;
        newBaseMinute = newBaseMinute % 60;

        // 格式化输出到分钟
        return String.format("%d年%02d月%02d日%02d点%02d分", year, month, day, finalHour, newBaseMinute);
    }
//    static String createTimeKey(String tradeTime) {
//        // trade time format: yyyyMMddHHmmSSsss
//        // extract y, M, d, H, m, S, s
//        int year = Integer.parseInt(tradeTime.substring(0, 4));
//        int month = Integer.parseInt(tradeTime.substring(4, 6));
//        int day = Integer.parseInt(tradeTime.substring(6, 8));
//        int hour = Integer.parseInt(tradeTime.substring(8, 10));
//        int minute = Integer.parseInt(tradeTime.substring(10, 12));
//
//        // 基准时间设置为 9:30
//        int baseHour = 9;
//        int baseMinute = 30;
//
//        // 将输入的时间转换为绝对分钟数
//        int inputTotalMinutes = (hour * 60 + minute);
//
//        // 将 9:30 转换为绝对分钟数
//        int baseTotalMinutes = (baseHour * 60 + baseMinute);
//        int timeSlotIndex = 0;
//        // 计算时间间隔
//        if (inputTotalMinutes < baseTotalMinutes) {
//            // 输入时间在 9:30 之前，时间间隔为 0
//            timeSlotIndex = (inputTotalMinutes - baseTotalMinutes-10) / (10);
//        } else {
//            timeSlotIndex = (inputTotalMinutes - baseTotalMinutes) / (10);
//        }
//          // 每个时间段为 10 分钟
////         System.out.println("timeSlotIndex: " + timeSlotIndex);
//        // 计算新的分钟数，返回的时间点为下一个时间段的起始时间
//        int newBaseMinute = baseMinute + timeSlotIndex * 10;
//
//        // 如果 newBaseMinute 超过 59 分钟，需要进位到小时
//        int finalHour = baseHour + newBaseMinute / 60;
//        newBaseMinute = newBaseMinute % 60;
//
//        // 格式化输出到分钟
//        return String.format("%d年%02d月%02d日%02d点%02d分", year, month, day, finalHour, newBaseMinute);
//    }

}
