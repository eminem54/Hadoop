package HW1;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Random;
import java.util.Scanner;
public class Main2 {
    public static void main(String[] args) throws IOException {
        PrintWriter output = new PrintWriter("data.txt");
        Scanner sc = new Scanner(System.in);
        int peopleNum;
        int dataCnt;
        System.out.println("몇명분의데이터?");
        peopleNum = sc.nextInt();
        System.out.println("데이터몇라인?");
        dataCnt = sc.nextInt();
        
        Random rand = new Random();
        HashMap<String, String> map = new HashMap<String, String>();
        for(int i = 1; i<peopleNum+1; i++) {
            map.put(Integer.toString(i), "0" + Integer.toString(1011111111 + i*1234567));
        }

        for(int i = 0; i<dataCnt; i++) {
            int number = rand.nextInt(peopleNum)+1;
            String dataline = Integer.toString(number) +" " + map.get(Integer.toString(number)) + " " + getRandTime() + " " + getRandBus(rand.nextInt(25));
            output.println(dataline);
        }
        output.close();
    }
    
    public static char getRandBus(int num) {
        char ret = (char) ('A' + (char)num);
        return ret;
    }
    
    public static String getRandTime() {
        Random random1 = new Random();
        int hour = random1.nextInt(12)+1;
        int minute = random1.nextInt(60)+1;
        if(hour < 10) {
            if(minute<10) return "0" + hour + ":" + "0" + minute;
            return "0" + hour + ":" + minute;
        }
        if(minute<10)return "" + hour + ":" + "0" + minute;
        return "" + hour + ":" + minute;
    }
}





