import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class Main {
  public static void main(String[] args){
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    System.out.println("Logs from your program will appear here!");

    //  Uncomment the code below to pass the first stage
        ServerSocket serverSocket = null;
        int port = 6379;

        List<Socket> clients = new ArrayList<>();

        try {
          serverSocket = new ServerSocket(port);
          // Since the tester restarts your program quite often, setting SO_REUSEADDR
          // ensures that we don't run into 'Address already in use' errors
          serverSocket.setReuseAddress(true);
          // Wait for connection from client.

          Map<String, String> map = new ConcurrentHashMap<>();

          Map<String, Date> mapTime = new ConcurrentHashMap<>();

          List<String> list = Collections.synchronizedList(new ArrayList<>());

          Map<String, CopyOnWriteArrayList<String>> mapList = new ConcurrentHashMap<>();

          Lock lock = new ReentrantLock(true);

          Lock streamlock = new ReentrantLock(true);

          Map<String, Condition> condList = new ConcurrentHashMap<>();

          Map<String, CopyOnWriteArrayList<ConcurrentHashMap<String, Object>>> streamMap = new ConcurrentHashMap<>();

          Map<String, String> streamDolorMap = new ConcurrentHashMap<>();

          Map<String, Boolean> multiMap = new ConcurrentHashMap<>();

          Queue<String> que = new LinkedList<>();


            while (true) {
                Socket clientSocket = serverSocket.accept();
                new Thread(() -> {
                    try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                         PrintWriter printWriter = new PrintWriter(clientSocket.getOutputStream(), true)) {

                        String message;
                        while ((message = bufferedReader.readLine()) != null) {
                            if (message.startsWith("*")) {
                                int length = Integer.parseInt(message.substring(1));
                                if (length > 0) {
                                    List<String> aa = new ArrayList<>();
                                    for (int i = 0; i < length; i++) {
                                        int l = Integer.parseInt(bufferedReader.readLine().substring(1));
                                        if (l == -1) {
                                            aa.add(null);
                                            continue;
                                        }
                                        String m = bufferedReader.readLine();
                                        aa.add(m);
                                    }

                                    for (int i = 0; i < aa.size(); i++) {
                                        if (aa.get(i).equals("PING")) {
                                            printWriter.print("+PONG" + "\r\n");
                                            printWriter.flush();
                                        } else if (aa.get(i).equals("ECHO")) {
                                            printWriter.print("$" + aa.get(i + 1).length() + "\r\n" + aa.get(i + 1) + "\r\n");
                                            printWriter.flush();
                                        } else if (aa.get(i).equals("SET")) {
                                            if (multiMap.get(Thread.currentThread().getName()) != null && multiMap.get(Thread.currentThread().getName())) {
                                                printWriter.print("+QUEUED\r\n");
                                                printWriter.flush();
                                                if (i + 3 < aa.size()) {
                                                    que.add(aa.get(i) + " " + aa.get(i + 1) + " " + aa.get(i + 2) + " "
                                                            + aa.get(i + 3) + " " + aa.get(i + 4));
                                                } else {
                                                    que.add(aa.get(i) + " " + aa.get(i + 1) + " " + aa.get(i + 2));
                                                }
                                                continue;
                                            }
                                            if (i + 3 < aa.size()
                                                    && (aa.get(i + 3).equalsIgnoreCase("px")
                                                    || aa.get(i + 3).equalsIgnoreCase("ex"))) {
                                                Date date = aa.get(i + 3).equalsIgnoreCase("px")
                                                        ? new Date(System.currentTimeMillis() + Long.parseLong(aa.get(i + 4)))
                                                        : new Date(System.currentTimeMillis() + Long.parseLong(aa.get(i + 4)) * 1000);
                                                mapTime.put(aa.get(i + 1), date);
                                            }
                                            map.put(aa.get(i + 1), aa.get(i + 2));
                                            printWriter.print("+OK" + "\r\n");
                                            printWriter.flush();
                                        } else if (aa.get(i).equals("GET")) {
                                            if (map.containsKey(aa.get(i + 1)) && !mapTime.containsKey(aa.get(i + 1))) {
                                                printWriter.print("$" + map.get(aa.get(i + 1)).length() + "\r\n" + map.get(aa.get(i + 1)) + "\r\n");
                                                printWriter.flush();
                                            } else if (map.containsKey(aa.get(i + 1)) && mapTime.containsKey(aa.get(i + 1))) {
                                                if (mapTime.get(aa.get(i + 1)).before(new Date())) {
                                                    printWriter.print("$-1\r\n");
                                                    printWriter.flush();
                                                    map.remove(aa.get(i + 1));
                                                    mapTime.remove(aa.get(i + 1));
                                                } else {
                                                    printWriter.print("$" + map.get(aa.get(i + 1)).length() + "\r\n" + map.get(aa.get(i + 1)) + "\r\n");
                                                    printWriter.flush();
                                                }
                                            } else {
                                                printWriter.print("$-1\r\n");
                                                printWriter.flush();
                                            }
                                        } else if (aa.get(i).equals("RPUSH")) {
                                            String key = aa.get(i + 1);
                                            lock.lock();
                                            try {
                                                CopyOnWriteArrayList<String> tmpList = mapList.computeIfAbsent(key, k -> new CopyOnWriteArrayList<>());
                                                for (int j = i + 2; j < length; j++) {
                                                    tmpList.add(aa.get(j));
                                                }
                                                printWriter.print(":" + tmpList.size() + "\r\n");
                                                printWriter.flush();
                                                Condition conditionMet = condList.get(key);
                                                if (conditionMet != null) {
                                                    conditionMet.signalAll();
                                                }
                                            } finally {
                                                lock.unlock();
                                            }
                                        } else if (aa.get(i).equals("LRANGE")) {
                                            CopyOnWriteArrayList<String> tmpList = mapList.getOrDefault(aa.get(i + 1), null);
                                            int f = Integer.parseInt(aa.get(i + 2));
                                            int t = Integer.parseInt(aa.get(i + 3));
                                            if (tmpList == null || f > t && f >= 0 && t >= 0 || f >= tmpList.size()) {
                                                printWriter.print("*0\r\n");
                                                printWriter.flush();
                                            } else {
                                                if (f < -tmpList.size()) {
                                                    f = 0;
                                                } else if (f < 0) {
                                                    f = tmpList.size() + f;
                                                }
                                                if (t < 0) {
                                                    t = tmpList.size() + t;
                                                }
                                                List<String> tmpList2 = tmpList.subList(f, t >= tmpList.size() ? tmpList.size() : t + 1);
                                                printWriter.print("*" + tmpList2.size() + "\r\n");
                                                for (int j = 0; j < tmpList2.size(); j++) {
                                                    printWriter.print("$" + tmpList2.get(j).length() + "\r\n");
                                                    printWriter.print(tmpList2.get(j) + "\r\n");
                                                }
                                                printWriter.flush();
                                            }
                                        } else if (aa.get(i).equals("LPUSH")) {
                                            String key = aa.get(i + 1);
                                            lock.lock();
                                            try {
                                                CopyOnWriteArrayList<String> tmpList = mapList.computeIfAbsent(key, k -> new CopyOnWriteArrayList<>());
                                                for (int j = i + 2; j < length; j++) {
                                                    tmpList.addFirst(aa.get(j));
                                                }
                                                printWriter.print(":" + tmpList.size() + "\r\n");
                                                printWriter.flush();
                                                Condition conditionMet = condList.get(key);
                                                if (conditionMet != null) {
                                                    conditionMet.signalAll();
                                                }
                                            } finally {
                                                lock.unlock();
                                            }
                                        } else if (aa.get(i).equals("LLEN")) {
                                            CopyOnWriteArrayList<String> tmpList = mapList.getOrDefault(aa.get(i + 1), null);
                                            if (tmpList == null) {
                                                printWriter.print(":0\r\n");
                                                printWriter.flush();
                                            } else {
                                                printWriter.print(":" + tmpList.size() + "\r\n");
                                                printWriter.flush();
                                            }
                                        } else if (aa.get(i).equals("LPOP")) {
                                            CopyOnWriteArrayList<String> tmpList = mapList.computeIfAbsent(aa.get(i + 1), k -> new CopyOnWriteArrayList<>());
                                            if (tmpList.isEmpty()) {
                                                printWriter.print("$-1\r\n");
                                                printWriter.flush();
                                            } else {
                                                int n = length == 3 ? Integer.parseInt(aa.get(i + 2)) : 1;
                                                if (n > 1) {
                                                    if (n > tmpList.size()) {
                                                        n = tmpList.size();
                                                    }
                                                    printWriter.print("*" + n + "\r\n");
                                                    for (int j = 0; j < n; j++) {
                                                        String s = tmpList.removeFirst();
                                                        printWriter.print("$" + s.length() + "\r\n" + s + "\r\n");
                                                    }
                                                } else {
                                                    String s = tmpList.removeFirst();
                                                    printWriter.print("$" + s.length() + "\r\n" + s + "\r\n");
                                                }
                                                printWriter.flush();
                                            }
                                        } else if (aa.get(i).equals("BLPOP")) {
                                            String key = aa.get(i + 1);
                                            long btMillSec = length == 3 ? (long) (Double.parseDouble(aa.get(i + 2)) * 1000L) : 0;
                                            lock.lock();
                                            try {
                                                Condition conditionMet = condList.computeIfAbsent(key, k -> lock.newCondition());
                                                boolean timedOut = false;
                                                while (mapList.getOrDefault(key, new CopyOnWriteArrayList<>()).isEmpty()) {
                                                    if (btMillSec == 0) {
                                                        conditionMet.await();
                                                    } else {
                                                        if (!conditionMet.await(btMillSec, TimeUnit.MILLISECONDS)) {
                                                            timedOut = true;
                                                            break;
                                                        }
                                                    }
                                                }

                                                if (timedOut) {
                                                    printWriter.print("*-1\r\n");
                                                } else {
                                                    CopyOnWriteArrayList<String> tmpList = mapList.get(key);
                                                    String s = tmpList.removeFirst();
                                                    printWriter.print("*2\r\n");
                                                    printWriter.print("$" + key.length() + "\r\n" + key + "\r\n");
                                                    printWriter.print("$" + s.length() + "\r\n" + s + "\r\n");
                                                }
                                                printWriter.flush();
                                            } catch (InterruptedException e) {
                                                Thread.currentThread().interrupt();
                                            } finally {
                                                lock.unlock();
                                            }
                                        } else if (aa.get(i).equals("TYPE")) {
                                            String key = aa.get(i + 1);
                                            if (map.containsKey(key)) {
                                                printWriter.print("+string\r\n");
                                                printWriter.flush();
                                            } else if (streamMap.containsKey(key)) {
                                                printWriter.print("+stream\r\n");
                                                printWriter.flush();
                                            } else {
                                                printWriter.print("+none\r\n");
                                                printWriter.flush();
                                            }
                                        } else if (aa.get(i).equals("XADD")) {
                                            String key = aa.get(i + 1);
                                            streamlock.lock();

                                            try {
                                                CopyOnWriteArrayList<ConcurrentHashMap<String, Object>> tmpList = streamMap.computeIfAbsent(key, k -> new CopyOnWriteArrayList<>());
                                                if (tmpList.isEmpty()) {
                                                    ConcurrentHashMap<String, Object> m = new ConcurrentHashMap<>();
                                                    for (int j = i + 1; j < length; j += 2) {
                                                        if (j == i + 1) {
                                                            if (aa.get(j + 1).endsWith("-*")) {
                                                                String newPre = aa.get(j + 1).substring(0, aa.get(j + 1).length() - 2);
                                                                if (newPre.equals("0")) {
                                                                    m.put("id", newPre + "-1");
                                                                } else {
                                                                    m.put("id", newPre + "-0");
                                                                }

                                                            } else if (aa.get(j + 1).startsWith("*")) {
                                                                m.put("id", System.currentTimeMillis() + "-0");
                                                            } else {
                                                                m.put("id", aa.get(j + 1));
                                                            }
                                                        } else {
                                                            m.put(aa.get(j), aa.get(j + 1));
                                                        }
                                                    }
                                                    tmpList.add(m);
                                                    printWriter.print("$" + String.valueOf(m.get("id")).length() + "\r\n" + m.get("id") + "\r\n");
                                                    printWriter.flush();

                                                } else {
                                                    ConcurrentHashMap<String, Object> last = tmpList.getLast();
                                                    String lastId = (String) last.get("id");
                                                    String lastPre = lastId.substring(0, lastId.lastIndexOf("-"));
                                                    int lastSuff = Integer.parseInt(lastId.substring(lastId.lastIndexOf("-") + 1));

                                                    String newId = aa.get(i + 2);
                                                    String newPre = newId.substring(0, lastId.lastIndexOf("-"));
                                                    int newSuff = 0;

                                                    if (newId.endsWith("-*")) {
                                                        if (lastPre.equals(newPre)) {
                                                            newSuff = lastSuff + 1;
                                                            newId = newPre + "-" + newSuff;
                                                        } else if (newPre.compareTo(lastPre) < 0) {
                                                            printWriter.print("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n");
                                                            printWriter.flush();
                                                            continue;
                                                        } else {
                                                            newId = newPre + "-0";
                                                        }
                                                    } else if (newId.startsWith("*")) {
                                                        long mill = System.currentTimeMillis();
                                                        if (lastPre.equals(String.valueOf(mill))) {
                                                            newId = lastPre + "-" + (lastSuff + 1);
                                                        } else {
                                                            newId = mill + "-0";
                                                        }
                                                    }
                                                    if (newId.equals("0-0")) {
                                                        printWriter.print("-ERR The ID specified in XADD must be greater than 0-0\r\n");
                                                        printWriter.flush();
                                                    } else if (newId.compareTo(lastId) <= 0) {
                                                        printWriter.print("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n");
                                                        printWriter.flush();
                                                    } else {
                                                        ConcurrentHashMap<String, Object> m = new ConcurrentHashMap<>();
                                                        for (int j = i + 1; j < length; j += 2) {
                                                            if (j == i + 1) {
                                                                m.put("id", newId);
                                                            } else {
                                                                m.put(aa.get(j), aa.get(j + 1));
                                                            }
                                                        }
                                                        tmpList.add(m);
                                                        printWriter.print("$" + String.valueOf(m.get("id")).length() + "\r\n" + m.get("id") + "\r\n");
                                                        printWriter.flush();
                                                    }

                                                }
                                                Condition conditionMet = condList.get(key);
                                                if (conditionMet != null) {
                                                    conditionMet.signalAll();
                                                }
                                            } finally {
                                                streamlock.unlock();
                                            }

                                        } else if (aa.get(i).equals("XRANGE")) {
                                            String key = aa.get(i + 1);
                                            CopyOnWriteArrayList<ConcurrentHashMap<String, Object>> tmpList = streamMap.getOrDefault(key,null);
                                            if (tmpList == null || tmpList.isEmpty()) {
                                                printWriter.print("*0\r\n");
                                                printWriter.flush();
                                            } else {
                                                String st = aa.get(i + 2);
                                                String en = aa.get(i + 3);
                                                long f = 0;
                                                long fi = 0;
                                                long t = Long.MAX_VALUE;
                                                long ti = Long.MAX_VALUE;
                                                if (!st.equals("-")) {
                                                    f = st.contains("-") ? Long.parseLong(st.substring(0, st.lastIndexOf("-"))) : Long.parseLong(st);
                                                    fi = st.contains("-") ? Long.parseLong(st.substring(st.lastIndexOf("-") + 1)) : 0;
                                                }
                                                if (!en.equals("+")) {
                                                    t = en.contains("-") ? Long.parseLong(en.substring(0, en.lastIndexOf("-"))) : Long.parseLong(en);
                                                    ti = en.contains("-") ? Long.parseLong(en.substring(en.lastIndexOf("-") + 1)) : Long.MAX_VALUE;
                                                }
                                                
                                                CopyOnWriteArrayList<ConcurrentHashMap<String, Object>> resList = new CopyOnWriteArrayList<>();
                                                for (int x = 0; x < tmpList.size(); x++) {
                                                    String idString = String.valueOf(tmpList.get(x).get("id"));
                                                    long ids = Long.parseLong(idString.substring(0, idString.lastIndexOf("-")));
                                                    long suffs = Long.parseLong(idString.substring(idString.lastIndexOf("-") + 1));

                                                    boolean isAfterStart = (ids > f) || (ids == f && suffs >= fi);
                                                    boolean isBeforeEnd = (ids < t) || (ids == t && suffs <= ti);
                                                    if (isAfterStart && isBeforeEnd) {
                                                        resList.add(tmpList.get(x));
                                                    }
                                                }
                                                printWriter.print("*" + resList.size() + "\r\n");
                                                for (ConcurrentHashMap<String, Object> tm : resList) {
                                                    printWriter.print("*2\r\n");
                                                    printWriter.print("$" + String.valueOf(tm.get("id")).length() + "\r\n" + tm.get("id") + "\r\n");
                                                    printWriter.print("*" + (tm.size() - 1) * 2 + "\r\n");
                                                    for (Map.Entry<String, Object> entry : tm.entrySet()) {
                                                        if (!entry.getKey().equals("id")) {
                                                            printWriter.print("$" + entry.getKey().length() + "\r\n" + entry.getKey() + "\r\n");
                                                            printWriter.print("$" + String.valueOf(entry.getValue()).length() + "\r\n" + entry.getValue() + "\r\n");
                                                        }
                                                    }
                                                }
                                                printWriter.flush();
                                            }
                                        } else if (aa.get(i).equals("XREAD")) {
                                            String type = aa.get(i + 1);
                                            if (type.equalsIgnoreCase("STREAMS")) {
                                                int n = (length - 2) / 2;

                                                StringBuffer sb = new StringBuffer();
                                                sb.append("*" + n + "\r\n");

                                                for (int e = 0; e < n; e++) {
                                                    String key = aa.get(i + 2 + e);
                                                    String st = aa.get(i + 2 + e + n);
                                                    long f = st.contains("-") ? Long.parseLong(st.substring(0, st.lastIndexOf("-"))) : Long.parseLong(st);
                                                    long fi = st.contains("-") ? Long.parseLong(st.substring(st.lastIndexOf("-") + 1)) : 0;

                                                    CopyOnWriteArrayList<ConcurrentHashMap<String, Object>> tmpList = streamMap.getOrDefault(key,null);
                                                    if (tmpList == null || tmpList.isEmpty()) {
                                                        sb.append("*2\r\n");
                                                        sb.append("$" + key.length() + "\r\n" + key + "\r\n");
                                                        sb.append("*-1\r\n");
                                                    } else {
                                                        CopyOnWriteArrayList<ConcurrentHashMap<String, Object>> resList = new CopyOnWriteArrayList<>();
                                                        for (int x = 0; x < tmpList.size(); x++) {
                                                            String idString = String.valueOf(tmpList.get(x).get("id"));
                                                            long ids = Long.parseLong(idString.substring(0, idString.lastIndexOf("-")));
                                                            long suffs = Long.parseLong(idString.substring(idString.lastIndexOf("-") + 1));

                                                            boolean isAfterStart = (ids > f) || (ids == f && suffs > fi);
                                                            if (isAfterStart) {
                                                                resList.add(tmpList.get(x));
                                                            }
                                                        }
                                                        sb.append("*2\r\n");
                                                        sb.append("$" + key.length() + "\r\n" + key + "\r\n");
                                                        sb.append("*" + resList.size() + "\r\n");
                                                        for (ConcurrentHashMap<String, Object> tm : resList) {
                                                            sb.append("*2\r\n");
                                                            sb.append("$" + String.valueOf(tm.get("id")).length() + "\r\n" + tm.get("id") + "\r\n");
                                                            sb.append("*" + (tm.size() - 1) * 2 + "\r\n");
                                                            for (Map.Entry<String, Object> entry : tm.entrySet()) {
                                                                if (!entry.getKey().equals("id")) {
                                                                    sb.append("$" + entry.getKey().length() + "\r\n" + entry.getKey() + "\r\n");
                                                                    sb.append("$" + String.valueOf(entry.getValue()).length() + "\r\n" + entry.getValue() + "\r\n");
                                                                }
                                                            }
                                                        }

                                                    }

                                                }

                                                printWriter.print(sb);
                                                printWriter.flush();
                                            } else if (type.equalsIgnoreCase("BLOCK")) {
                                                long timeOut = Long.parseLong(aa.get(i + 2));
                                                String type2 = aa.get(i + 3);
                                                if (!type2.equalsIgnoreCase("STREAMS")) {
                                                    continue;
                                                }

                                                streamlock.lock();

                                                try {
                                                    int n = (length - 4) / 2;

                                                    StringBuffer sb = new StringBuffer();
                                                    sb.append("*" + n + "\r\n");

                                                    for (int e = 0; e < n; e++) {
                                                        String key = aa.get(i + 4 + e);
                                                        String st = aa.get(i + 4 + e + n);
                                                        long f = 0;
                                                        long fi = 0;
                                                        if (!st.equals("$")) {
                                                            f = st.contains("-") ? Long.parseLong(st.substring(0, st.lastIndexOf("-"))) : Long.parseLong(st);
                                                            fi = st.contains("-") ? Long.parseLong(st.substring(st.lastIndexOf("-") + 1)) : 0;
                                                        }
                                                        Condition conditionMet = condList.computeIfAbsent(key, k -> streamlock.newCondition());
                                                        boolean flag = false;

                                                        CopyOnWriteArrayList<ConcurrentHashMap<String, Object>> resList = new CopyOnWriteArrayList<>();
                                                        while (!flag && timeOut == 0) {
                                                            CopyOnWriteArrayList<ConcurrentHashMap<String, Object>> ma = streamMap.getOrDefault(key, new CopyOnWriteArrayList<>());
                                                            if (ma.isEmpty()) {
                                                                streamDolorMap.put(key, "0-0");
                                                                conditionMet.await();
                                                                continue;
                                                            } else {
                                                                if (st.equals("$") && (streamDolorMap.get(key) == null || streamDolorMap.get(key).isEmpty())) {
                                                                    streamDolorMap.put(key, String.valueOf(ma.getLast().get("id")));
                                                                    conditionMet.await();
                                                                    continue;
                                                                } else if (st.equals("$") && streamDolorMap.get(key).equals("0-0")) {
                                                                    sb.append("*2\r\n");
                                                                    sb.append("$" + key.length() + "\r\n" + key + "\r\n");
                                                                    sb.append("*" + ma.size() + "\r\n");
                                                                    for (ConcurrentHashMap<String, Object> tm : ma) {
                                                                        sb.append("*2\r\n");
                                                                        sb.append("$" + String.valueOf(tm.get("id")).length() + "\r\n" + tm.get("id") + "\r\n");
                                                                        sb.append("*" + (tm.size() - 1) * 2 + "\r\n");
                                                                        for (Map.Entry<String, Object> entry : tm.entrySet()) {
                                                                            if (!entry.getKey().equals("id")) {
                                                                                sb.append("$" + entry.getKey().length() + "\r\n" + entry.getKey() + "\r\n");
                                                                                sb.append("$" + String.valueOf(entry.getValue()).length() + "\r\n" + entry.getValue() + "\r\n");
                                                                            }
                                                                        }
                                                                    }
                                                                    break;
                                                                } else if (st.equals("$")) {
                                                                    ma = ma.stream().
                                                                            filter(m -> String.valueOf(m.get("id")).compareTo(streamDolorMap.get(key)) > 0)
                                                                            .sorted(Comparator.comparing(ms -> String.valueOf(ms.get("id"))))
                                                                            .collect(Collectors.toCollection(CopyOnWriteArrayList::new));
                                                                    sb.append("*2\r\n");
                                                                    sb.append("$" + key.length() + "\r\n" + key + "\r\n");
                                                                    sb.append("*" + ma.size() + "\r\n");
                                                                    for (ConcurrentHashMap<String, Object> tm : ma) {
                                                                        sb.append("*2\r\n");
                                                                        sb.append("$" + String.valueOf(tm.get("id")).length() + "\r\n" + tm.get("id") + "\r\n");
                                                                        sb.append("*" + (tm.size() - 1) * 2 + "\r\n");
                                                                        for (Map.Entry<String, Object> entry : tm.entrySet()) {
                                                                            if (!entry.getKey().equals("id")) {
                                                                                sb.append("$" + entry.getKey().length() + "\r\n" + entry.getKey() + "\r\n");
                                                                                sb.append("$" + String.valueOf(entry.getValue()).length() + "\r\n" + entry.getValue() + "\r\n");
                                                                            }
                                                                        }
                                                                    }
                                                                    break;
                                                                }
                                                                for (int x = 0; x < ma.size(); x++) {
                                                                    String idString = String.valueOf(ma.get(x).get("id"));
                                                                    long ids = Long.parseLong(idString.substring(0, idString.lastIndexOf("-")));
                                                                    long suffs = Long.parseLong(idString.substring(idString.lastIndexOf("-") + 1));

                                                                    boolean isAfterStart = (ids > f) || (ids == f && suffs > fi);
                                                                    if (isAfterStart) {
                                                                        resList.add(ma.get(x));
                                                                    }
                                                                }
                                                                if (resList.isEmpty()) {
                                                                    conditionMet.await();
                                                                    continue;
                                                                } else {
                                                                    flag = true;
                                                                    sb.append("*2\r\n");
                                                                    sb.append("$" + key.length() + "\r\n" + key + "\r\n");
                                                                    sb.append("*" + resList.size() + "\r\n");
                                                                    for (ConcurrentHashMap<String, Object> tm : resList) {
                                                                        sb.append("*2\r\n");
                                                                        sb.append("$" + String.valueOf(tm.get("id")).length() + "\r\n" + tm.get("id") + "\r\n");
                                                                        sb.append("*" + (tm.size() - 1) * 2 + "\r\n");
                                                                        for (Map.Entry<String, Object> entry : tm.entrySet()) {
                                                                            if (!entry.getKey().equals("id")) {
                                                                                sb.append("$" + entry.getKey().length() + "\r\n" + entry.getKey() + "\r\n");
                                                                                sb.append("$" + String.valueOf(entry.getValue()).length() + "\r\n" + entry.getValue() + "\r\n");
                                                                            }
                                                                        }
                                                                    }
                                                                }
                                                            }

                                                        }

                                                        boolean res = false;
                                                        boolean b = false;
                                                        Date line = new Date(System.currentTimeMillis() + timeOut);
                                                        while (!res && timeOut > 0) {
                                                            CopyOnWriteArrayList<ConcurrentHashMap<String, Object>> ma = streamMap.getOrDefault(key, new CopyOnWriteArrayList<>());
                                                            if (ma.isEmpty()) {
                                                                streamDolorMap.put(key, "0-0");
                                                                b = conditionMet.awaitUntil(line);
                                                                if (!b) {
                                                                    break;
                                                                }
                                                                continue;
                                                            } else {
                                                                if (st.equals("$") && (streamDolorMap.get(key) == null || streamDolorMap.get(key).isEmpty())) {
                                                                    streamDolorMap.put(key, String.valueOf(ma.getLast().get("id")));
                                                                    b = conditionMet.awaitUntil(line);
                                                                    if (!b) {
                                                                        break;
                                                                    }
                                                                    continue;
                                                                } else if (st.equals("$") && streamDolorMap.get(key).equals("0-0")) {
                                                                    sb.append("*2\r\n");
                                                                    sb.append("$" + key.length() + "\r\n" + key + "\r\n");
                                                                    sb.append("*" + ma.size() + "\r\n");
                                                                    for (ConcurrentHashMap<String, Object> tm : ma) {
                                                                        sb.append("*2\r\n");
                                                                        sb.append("$" + String.valueOf(tm.get("id")).length() + "\r\n" + tm.get("id") + "\r\n");
                                                                        sb.append("*" + (tm.size() - 1) * 2 + "\r\n");
                                                                        for (Map.Entry<String, Object> entry : tm.entrySet()) {
                                                                            if (!entry.getKey().equals("id")) {
                                                                                sb.append("$" + entry.getKey().length() + "\r\n" + entry.getKey() + "\r\n");
                                                                                sb.append("$" + String.valueOf(entry.getValue()).length() + "\r\n" + entry.getValue() + "\r\n");
                                                                            }
                                                                        }
                                                                    }
                                                                    break;
                                                                } else if (st.equals("$")) {
                                                                    ma = ma.stream().
                                                                            filter(m -> String.valueOf(m.get("id")).compareTo(streamDolorMap.get(key)) > 0)
                                                                            .sorted(Comparator.comparing(ms -> String.valueOf(ms.get("id"))))
                                                                            .collect(Collectors.toCollection(CopyOnWriteArrayList::new));
                                                                    sb.append("*2\r\n");
                                                                    sb.append("$" + key.length() + "\r\n" + key + "\r\n");
                                                                    sb.append("*" + ma.size() + "\r\n");
                                                                    for (ConcurrentHashMap<String, Object> tm : ma) {
                                                                        sb.append("*2\r\n");
                                                                        sb.append("$" + String.valueOf(tm.get("id")).length() + "\r\n" + tm.get("id") + "\r\n");
                                                                        sb.append("*" + (tm.size() - 1) * 2 + "\r\n");
                                                                        for (Map.Entry<String, Object> entry : tm.entrySet()) {
                                                                            if (!entry.getKey().equals("id")) {
                                                                                sb.append("$" + entry.getKey().length() + "\r\n" + entry.getKey() + "\r\n");
                                                                                sb.append("$" + String.valueOf(entry.getValue()).length() + "\r\n" + entry.getValue() + "\r\n");
                                                                            }
                                                                        }
                                                                    }
                                                                    break;
                                                                }
                                                                for (int x = 0; x < ma.size(); x++) {
                                                                    String idString = String.valueOf(ma.get(x).get("id"));
                                                                    long ids = Long.parseLong(idString.substring(0, idString.lastIndexOf("-")));
                                                                    long suffs = Long.parseLong(idString.substring(idString.lastIndexOf("-") + 1));

                                                                    boolean isAfterStart = (ids > f) || (ids == f && suffs > fi);
                                                                    if (isAfterStart) {
                                                                        resList.add(ma.get(x));
                                                                    }
                                                                }
                                                                if (resList.isEmpty()) {
                                                                    b = conditionMet.awaitUntil(line);
                                                                    if (!b) {
                                                                        break;
                                                                    }
                                                                    continue;
                                                                } else {
                                                                    res = true;
                                                                    sb.append("*2\r\n");
                                                                    sb.append("$" + key.length() + "\r\n" + key + "\r\n");
                                                                    sb.append("*" + resList.size() + "\r\n");
                                                                    for (ConcurrentHashMap<String, Object> tm : resList) {
                                                                        sb.append("*2\r\n");
                                                                        sb.append("$" + String.valueOf(tm.get("id")).length() + "\r\n" + tm.get("id") + "\r\n");
                                                                        sb.append("*" + (tm.size() - 1) * 2 + "\r\n");
                                                                        for (Map.Entry<String, Object> entry : tm.entrySet()) {
                                                                            if (!entry.getKey().equals("id")) {
                                                                                sb.append("$" + entry.getKey().length() + "\r\n" + entry.getKey() + "\r\n");
                                                                                sb.append("$" + String.valueOf(entry.getValue()).length() + "\r\n" + entry.getValue() + "\r\n");
                                                                            }
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                        }

                                                        if (timeOut > 0 && !b) {
                                                            sb.append("*2\r\n");
                                                            sb.append("$" + key.length() + "\r\n" + key + "\r\n");
                                                            sb.append("*-1\r\n");
                                                            if (n == 1) {
                                                                sb.delete(0, sb.length());
                                                                sb.append("*-1\r\n");
                                                            }
                                                            continue;
                                                        }


                                                    }

                                                    printWriter.print(sb);
                                                    printWriter.flush();
                                                } finally {
                                                    streamlock.unlock();
                                                }

                                            }


                                        } else if (aa.get(i).equals("INCR")) {
                                            if (multiMap.get(Thread.currentThread().getName()) != null && multiMap.get(Thread.currentThread().getName())) {
                                                printWriter.print("+QUEUED\r\n");
                                                printWriter.flush();
                                                que.add(aa.get(i) + " " + aa.get(i + 1));
                                                continue;
                                            }
                                            String key = aa.get(i + 1);
                                            if (map.containsKey(key)) {
                                                int val = 0;
                                                try {
                                                    val = Integer.parseInt(map.get(key));
                                                } catch (NumberFormatException e) {
                                                    printWriter.print("-ERR value is not an integer or out of range\r\n");
                                                    printWriter.flush();
                                                    continue;
                                                }
                                                map.put(key, String.valueOf(val + 1));
                                                printWriter.print(":" + (val + 1) + "\r\n");
                                                printWriter.flush();
                                            } else {
                                                map.put(key, "1");
                                                printWriter.print(":1\r\n");
                                                printWriter.flush();
                                            }

                                        } else if (aa.get(i).equals("MULTI")) {
                                            multiMap.put(Thread.currentThread().getName(), true);
                                            printWriter.print("+OK\r\n");
                                            printWriter.flush();
                                        } else if (aa.get(i).equals("EXEC")) {
                                            if (multiMap.containsKey(Thread.currentThread().getName()) && multiMap.get(Thread.currentThread().getName())) {
                                                if (que.isEmpty()) {
                                                    printWriter.print("*0\r\n");
                                                    printWriter.flush();
                                                    multiMap.remove(Thread.currentThread().getName());
                                                    continue;
                                                }
                                                while (!que.isEmpty()) {
                                                    String[] task = que.poll().split(" ");
                                                    if (task[0].equals("SET")) {
                                                        if (task.length > 3
                                                                && (task[3].equalsIgnoreCase("px")
                                                                || task[3].equalsIgnoreCase("ex"))) {
                                                            Date date = task[3].equalsIgnoreCase("px")
                                                                    ? new Date(System.currentTimeMillis() + Long.parseLong(task[4]))
                                                                    : new Date(System.currentTimeMillis() + Long.parseLong(task[4]) * 1000);
                                                            mapTime.put(task[1], date);
                                                        }
                                                        map.put(task[1], task[2]);
                                                        printWriter.print("+OK" + "\r\n");
                                                        printWriter.flush();
                                                    } else if (task[0].equals("INCR")) {
                                                        String key = task[1];
                                                        if (map.containsKey(key)) {
                                                            int val = 0;
                                                            try {
                                                                val = Integer.parseInt(map.get(key));
                                                            } catch (NumberFormatException e) {
                                                                printWriter.print("-ERR value is not an integer or out of range\r\n");
                                                                printWriter.flush();
                                                                continue;
                                                            }
                                                            map.put(key, String.valueOf(val + 1));
                                                            printWriter.print(":" + (val + 1) + "\r\n");
                                                            printWriter.flush();
                                                        } else {
                                                            map.put(key, "1");
                                                            printWriter.print(":1\r\n");
                                                            printWriter.flush();
                                                        }
                                                    }
                                                }
                                                multiMap.remove(Thread.currentThread().getName());
                                            } else {
                                                printWriter.print("-ERR EXEC without MULTI\r\n");
                                                printWriter.flush();
                                            }
                                        }


                                    }
                                } else {
                                    printWriter.flush();
                                }

                            } else {
                                printWriter.flush();
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("Exception: " + e.getMessage());
                    } finally {
                    }


                }).start();
            }
        } catch (IOException e) {
          System.out.println("IOException: " + e.getMessage());
        } finally {
        }
  }
}
