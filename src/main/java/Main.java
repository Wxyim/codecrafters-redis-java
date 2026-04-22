import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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

          Map<String, Condition> condList = new ConcurrentHashMap<>();

          Map<String, CopyOnWriteArrayList<ConcurrentHashMap<String, Object>>> streamMap = new ConcurrentHashMap<>();

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
                                            CopyOnWriteArrayList<ConcurrentHashMap<String, Object>> tmpList = streamMap.computeIfAbsent(key, k -> new CopyOnWriteArrayList<>());
                                            if (tmpList.isEmpty()) {
                                                ConcurrentHashMap<String, Object> m = new ConcurrentHashMap<>();
                                                for (int j = i + 1; j < length; j += 2) {
                                                    if (j == i + 1) {
                                                        if (aa.get(j + 1).endsWith("-*")) {
                                                            String newPre = aa.get(j + 1).substring(0, aa.get(j + 1).length() - 2);
                                                            if (newPre.equals("0")) {
                                                                m.put("id", newPre + "1");
                                                            } else {
                                                                m.put("id", newPre + "0");
                                                            }

                                                        } else if (aa.get(j + 1).startsWith("*")) {
                                                            m.put("id", System.currentTimeMillis() + "0");
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
                                                String id = (String) last.get("id");
                                                String newId = aa.get(i + 2);
                                                int suf = 0;
                                                if (aa.get(i + 2).endsWith("-*")) {
                                                    String lastPre = aa.get(i + 2).substring(0, aa.get(i + 2).length() - 2);
                                                    String newPre = newId.substring(0, newId.length() - 2);
                                                    if (lastPre.equals(newPre)) {
                                                        suf = Integer.parseInt(aa.get(i + 2).substring(aa.get(i + 2).lastIndexOf("-"))) + 1;
                                                        newId = newPre + "-" + suf;
                                                    } else if (newPre.compareTo(lastPre) < 0) {
                                                        printWriter.print("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n");
                                                        printWriter.flush();
                                                        continue;
                                                    }
                                                } else if (aa.get(i + 2).startsWith("*")) {
                                                    newId = System.currentTimeMillis() + "-0";
                                                }
                                                if (newId.equals("0-0")) {
                                                    printWriter.print("-ERR The ID specified in XADD must be greater than 0-0\r\n");
                                                    printWriter.flush();
                                                } else if (newId.compareTo(id) <= 0) {
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
