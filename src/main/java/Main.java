import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class Main {
    public static void main(String[] args){
        System.out.println("Logs from your program will appear here!");

        ServerSocket serverSocket = null;

        Map<String, Object> argsMap = new HashMap<>();
        AtomicBoolean isReplica = new AtomicBoolean(false);
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("--port")) {
                argsMap.put("port", Integer.parseInt(args[i + 1]));
            } else if (args[i].equals("--replicaof")) {
                argsMap.put("replicaof", args[i + 1]);
                isReplica.set(true);
            } else if (args[i].startsWith("--")) {
                argsMap.put(args[i].substring(2), args[i + 1]);
            }
        }

        int port = argsMap.containsKey("port") ? (int) argsMap.get("port") : 6379;

        Map<String, String> replMap = new ConcurrentHashMap<>();
        Map<String, Date> replMapTime = new ConcurrentHashMap<>();

        HashSet<String> subModCommands = new HashSet<>(Arrays.asList("subscribe", "unsubscribe", "psubscribe", "punsubscribe", "ping", "quit"));

        if (argsMap.containsKey("replicaof")) {
            String[] mainHost = ((String) argsMap.get("replicaof")).split(" ");
            // Replica connection handling — use PushbackInputStream and byte-level RESP parsing
            new Thread(() -> {
                try {
                    Socket masterSocket = new Socket(mainHost[0], Integer.parseInt(mainHost[1]));
                    masterSocket.setKeepAlive(true);

                    // Wrap input in PushbackInputStream so we can unread bytes when we peek
                    PushbackInputStream pin = new PushbackInputStream(masterSocket.getInputStream(), 8192);
                    PrintWriter printWriter = new PrintWriter(
                            new OutputStreamWriter(masterSocket.getOutputStream(), StandardCharsets.ISO_8859_1), true);

                    long replicaOffset = 0;
                    AtomicLong appliedOffset = new AtomicLong(0); // <-- track last applied offset
                    int handshakeState = 0;

                    // PING
                    printWriter.print("*1\r\n$4\r\nPING\r\n");
                    printWriter.flush();

                    String message;
                    while ((message = readLineFromStream(pin)) != null) {
                        if (message.isEmpty()) continue;

                        if (handshakeState < 4) {
                            if (handshakeState == 0 && message.startsWith("+PONG")) {
                                printWriter.print("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n" + port + "\r\n");
                                printWriter.flush();
                                handshakeState = 1;
                            } else if (handshakeState == 1 && message.startsWith("+OK")) {
                                printWriter.print("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n");
                                printWriter.flush();
                                handshakeState = 2;
                            } else if (handshakeState == 2 && message.startsWith("+OK")) {
                                printWriter.print("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n");
                                printWriter.flush();
                                handshakeState = 3;
                            } else if (message.startsWith("+FULLRESYNC")) {
                                handshakeState = 3;
                            } else if (handshakeState == 3 && message.startsWith("$")) {
                                // RDB length line
                                int rdbLength;
                                try {
                                    rdbLength = Integer.parseInt(message.substring(1));
                                } catch (NumberFormatException ex) {
                                    System.out.println("ERROR parsing RDB length: " + message);
                                    break;
                                }
                                // Read RDB binary exactly rdbLength bytes
                                byte[] rdbData = new byte[rdbLength];
                                int bytesRead = 0;
                                while (bytesRead < rdbLength) {
                                    int n = pin.read(rdbData, bytesRead, rdbLength - bytesRead);
                                    if (n == -1) break;
                                    bytesRead += n;
                                }
                                // Safely consume optional CRLF after bulk: peek one byte, if '\r' then read next and verify '\n'
                                int b1 = pin.read();
                                if (b1 == '\r') {
                                    int b2 = pin.read();
                                    if (b2 == '\n') {
                                        // ok consumed CRLF
                                    } else {
                                        if (b2 != -1) pin.unread(b2);
                                        pin.unread(b1);
                                    }
                                } else if (b1 != -1) {
                                    pin.unread(b1);
                                }
                                handshakeState = 4;
                                replicaOffset = 0;
                            }
                            continue;
                        }

                        // handshakeState == 4
                        byte[] lineBytes = message.getBytes(StandardCharsets.ISO_8859_1);
                        replicaOffset += lineBytes.length + 2;

                        if (message.startsWith("*")) {
                            int length;
                            try {
                                length = Integer.parseInt(message.substring(1));
                            } catch (NumberFormatException ex) {
                                System.out.println("ERROR: invalid array header: " + message);
                                continue;
                            }
                            System.out.println("DEBUG: Received command array length: " + length + ", message: " + message);
                            if (length > 0) {
                                List<String> aa = new ArrayList<>();
                                for (int i = 0; i < length; i++) {
                                    String lenLine = readLineFromStream(pin);
                                    while (lenLine != null && lenLine.isEmpty()) {
                                        lenLine = readLineFromStream(pin);
                                    }
                                    if (lenLine == null) {
                                        System.out.println("ERROR: Unexpected null lenLine at index " + i);
                                        break;
                                    }
                                    if (!lenLine.startsWith("$")) {
                                        System.out.println("ERROR: Expected $, got: " + lenLine);
                                        break;
                                    }

                                    replicaOffset += lenLine.getBytes(StandardCharsets.ISO_8859_1).length + 2;

                                    int l;
                                    try {
                                        l = Integer.parseInt(lenLine.substring(1));
                                    } catch (NumberFormatException ex) {
                                        System.out.println("ERROR parsing bulk length: " + lenLine);
                                        break;
                                    }
                                    if (l == -1) {
                                        aa.add(null);
                                        continue;
                                    }
                                    // Read l bytes of bulk data
                                    byte[] data = new byte[l];
                                    int read = 0;
                                    while (read < l) {
                                        int n = pin.read(data, read, l - read);
                                        if (n == -1) break;
                                        read += n;
                                    }
                                    // Safely consume optional CRLF after bulk
                                    int r = pin.read();
                                    if (r == '\r') {
                                        int s = pin.read();
                                        if (s == '\n') {
                                            // ok
                                        } else {
                                            if (s != -1) pin.unread(s);
                                            pin.unread(r);
                                        }
                                    } else if (r != -1) {
                                        pin.unread(r);
                                    }
                                    replicaOffset += l + 2;
                                    String m = new String(data, StandardCharsets.ISO_8859_1);
                                    aa.add(m);
                                }

                                System.out.println("DEBUG: Parsed command: " + aa + ", offset: " + replicaOffset);

                                // process tokens
                                for (int i = 0; i < aa.size(); i++) {
                                    if (aa.get(i) == null) continue;
                                    if ("SET".equalsIgnoreCase(aa.get(i))) {
                                        if (i + 3 < aa.size()
                                                && aa.get(i + 3) != null
                                                && (aa.get(i + 3).equalsIgnoreCase("px")
                                                || aa.get(i + 3).equalsIgnoreCase("ex"))) {
                                            Date date = aa.get(i + 3).equalsIgnoreCase("px")
                                                    ? new Date(System.currentTimeMillis() + Long.parseLong(aa.get(i + 4)))
                                                    : new Date(System.currentTimeMillis() + Long.parseLong(aa.get(i + 4)) * 1000);
                                            replMapTime.put(aa.get(i + 1), date);
                                        }
                                        replMap.put(aa.get(i + 1), aa.get(i + 2));

                                        // update appliedOffset AFTER applying this command, but do NOT send ACK here
                                        appliedOffset.set(replicaOffset);
                                        System.out.println("DEBUG: Applied SET, updated appliedOffset to: " + appliedOffset.get());
                                    } else if ("replconf".equalsIgnoreCase(aa.get(i))) {
                                        if (i + 2 < aa.size()
                                                && aa.get(i + 1) != null
                                                && aa.get(i + 2) != null
                                                && "getack".equalsIgnoreCase(aa.get(i + 1))
                                                && "*".equalsIgnoreCase(aa.get(i + 2))) {
                                            long ackOff = appliedOffset.get();
                                            System.out.println("DEBUG: Replica received GETACK, sending ACK with offset: " + ackOff);
                                            printWriter.print("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$"
                                                    + String.valueOf(ackOff).length() + "\r\n"
                                                    + ackOff + "\r\n");
                                            printWriter.flush();
                                        }
                                    }
                                }

                                // mark appliedOffset to include the whole RESP array (already done per command),
                                // keep this to ensure non-SET arrays (PING etc.) advance appliedOffset
                                appliedOffset.set(replicaOffset);
                            }
                        }
                    }
                } catch (IOException e) {
                    System.out.println("IOException in replica: " + e.getMessage());
                }
            }).start();
        }

        try {
            serverSocket = new ServerSocket(port);
            // Since the tester restarts your program quite often, setting SO_REUSEADDR
            // ensures that we don't run into 'Address already in use' errors
            serverSocket.setReuseAddress(true);

            Map<String, String> map = new ConcurrentHashMap<>();
            Map<String, Date> mapTime = new ConcurrentHashMap<>();
            Map<String, CopyOnWriteArrayList<String>> mapList = new ConcurrentHashMap<>();
            Lock lock = new ReentrantLock(true);
            Lock streamlock = new ReentrantLock(true);
            Map<String, Condition> condList = new ConcurrentHashMap<>();
            Map<String, CopyOnWriteArrayList<ConcurrentHashMap<String, Object>>> streamMap = new ConcurrentHashMap<>();
            Map<String, String> streamDolorMap = new ConcurrentHashMap<>();
            Map<String, Queue<String>> multiMap = new ConcurrentHashMap<>();
            Map<String, Map<String, Boolean>> watchMap = new ConcurrentHashMap<>();
            Map<Socket, LinkedBlockingQueue<String>> clientMap = new ConcurrentHashMap<>();

            AtomicInteger replicaCount = new AtomicInteger(0);
            Map<Socket, Long> replOffsetMap = new ConcurrentHashMap<>();
            AtomicLong lastOffset = new AtomicLong(0);
            Map<String, Long> replAckMap = new ConcurrentHashMap<>();
            AtomicLong commandEndOffset = new AtomicLong();

            Map<String, CopyOnWriteArraySet<String>> subMap = new ConcurrentHashMap<>();

            // 加载 rdb
            if (argsMap.containsKey("dir") && argsMap.containsKey("dbfilename")) {
                File file = new File(argsMap.get("dir") + "/" + argsMap.get("dbfilename"));
                if (file.isFile()) {
                    try (InputStream is = new FileInputStream(file);
                         ByteArrayOutputStream out = new ByteArrayOutputStream()) {

                        byte[] b = new byte[4096];
                        int len;
                        while ((len = is.read(b)) != -1) {
                            out.write(b, 0, len);
                        }

                        byte[] data = out.toByteArray();
                        ByteBuffer buf = ByteBuffer.wrap(data);

                        System.out.println("rdb read...");
                        // header
                        byte[] header = new byte[9];
                        buf.get(header);
                        System.out.println("header: " + new String(header, StandardCharsets.UTF_8));

                        byte op = buf.get();

                        while ((op & 0xFF) == 0xFA) {
                            String k = readBuf(buf);
                            System.out.println("metadata k:" + k);
                            String v = readBuf(buf);
                            System.out.println("metadata v:" + v);

                            op = buf.get();
                        }

                        if ((op & 0xFF) == 0xFE) {
                            byte dbidx = buf.get();
                            System.out.println("dbidx: " + dbidx);

                            op = buf.get();
                        }

                        if ((op & 0xFF) == 0xFB) {
                            int hashSize = readLenAsInt(buf);
                            System.out.println("hashSize:" + hashSize);
                            int expirySize = readLenAsInt(buf);
                            System.out.println("expirySize:" + expirySize);

                            op = buf.get();
                        }

                        // 数据解析循环也需要检查条件
                        while (op != (byte) 0xFF) {  // 0xFF 是 end marker
                            if ((op & 0xFF) == 0) {  // String type key-value
                                String k = readBuf(buf);
                                System.out.println("data k:" + k);
                                String v = readBuf(buf);
                                System.out.println("data v:" + v);
                                map.put(k, v);
                            } else if ((op & 0xFF) == 0xFC) {  // Expire time in milliseconds
                                buf.order(ByteOrder.LITTLE_ENDIAN);
                                long timeMills = buf.getLong();
                                buf.order(ByteOrder.BIG_ENDIAN);
                                System.out.println("data with timeMills: " + timeMills);
                                byte valueType = buf.get();  // value type, usually 0 for string
                                if (valueType == 0) {
                                    String k = readBuf(buf);
                                    System.out.println("data k:" + k);
                                    String v = readBuf(buf);
                                    System.out.println("data v:" + v);
                                    map.put(k, v);
                                    mapTime.put(k, new Date(timeMills));
                                }
                            } else if ((op & 0xFF) == 0xFD) {  // Expire time in seconds
                                buf.order(ByteOrder.LITTLE_ENDIAN);
                                int timeSecs = buf.getInt();
                                buf.order(ByteOrder.BIG_ENDIAN);
                                System.out.println("data with timeSecs: " + timeSecs);
                                byte valueType = buf.get();
                                if (valueType == 0) {
                                    String k = readBuf(buf);
                                    System.out.println("data k:" + k);
                                    String v = readBuf(buf);
                                    System.out.println("data v:" + v);
                                    map.put(k, v);
                                    mapTime.put(k, new Date(timeSecs * 1000L));
                                }
                            } else if ((op & 0xFF) == 0xFA) {
                                // Metadata section (shouldn't appear here, but handle it)
                                String k = readBuf(buf);
                                String v = readBuf(buf);
                            } else {
                                System.out.println("Unknown op code: " + String.format("0x%02X", op & 0xFF));
                                break;
                            }

                            try {
                                op = buf.get();
                            } catch (Exception e) {
                                break;  // End of buffer
                            }
                        }

                        if ((op & 0xFF) == 0xFF) {
                            byte[] crc = new byte[8];
                            if (buf.remaining() >= 8) {
                                buf.get(crc);
                                System.out.println("crc: " + Arrays.toString(crc));
                            }
                        }

                    } catch (Exception e) {
                        System.out.println("Error opening file: " + e.getLocalizedMessage());
                    }
                } else {
                    System.out.println("load RDB file : not founded");
                }
            }

            while (true) {
                Socket clientSocket = serverSocket.accept();
                clientSocket.setKeepAlive(true);
                AtomicBoolean isReplicaCli = new AtomicBoolean(false);
                AtomicBoolean isSubMod = new AtomicBoolean(false);
                Thread client = new Thread(() -> {
                    try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                         PrintWriter printWriter = new PrintWriter(clientSocket.getOutputStream(), true)) {

                        String message;
                        while ((message = bufferedReader.readLine()) != null) {
                            if (message != null && message.startsWith("*")) {
                                int length = Integer.parseInt(message.substring(1));
                                if (length > 0) {
                                    List<String> aa = new ArrayList<>();
                                    for (int i = 0; i < length; i++) {
                                        String lenLine = bufferedReader.readLine();
                                        if (lenLine == null) break;
                                        int l = Integer.parseInt(lenLine.substring(1));
                                        if (l == -1) {
                                            aa.add(null);
                                            continue;
                                        }
                                        String m = bufferedReader.readLine();
                                        aa.add(m);
                                    }

                                    for (int i = 0; i < aa.size(); i++) {

                                        if (isSubMod.get() && !subModCommands.contains(aa.getFirst().toLowerCase())) {
                                            printWriter.print("-ERR Can't execute '" + aa.get(i).toLowerCase() + "' in subscribed mode" + "\r\n");
                                            printWriter.flush();
                                            break;
                                        } else if (isSubMod.get() && "ping".equalsIgnoreCase(aa.getFirst())) {
                                            printWriter.print("*2\r\n$4\r\nPONG$0\r\n\r\n");
                                            printWriter.flush();
                                            break;
                                        }

                                        if (aa.get(i).equals("PING")) {
                                            printWriter.print("+PONG" + "\r\n");
                                            printWriter.flush();
                                        } else if (aa.get(i).equals("ECHO")) {
                                            printWriter.print("$" + aa.get(i + 1).length() + "\r\n" + aa.get(i + 1) + "\r\n");
                                            printWriter.flush();
                                        } else if (aa.get(i).equals("SET")) {
                                            Queue<String> que = multiMap.get(Thread.currentThread().getName());
                                            if (que != null) {
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
                                            boolean f = false;
                                            if (i + 3 < aa.size()
                                                    && (aa.get(i + 3).equalsIgnoreCase("px")
                                                    || aa.get(i + 3).equalsIgnoreCase("ex"))) {
                                                Date date = aa.get(i + 3).equalsIgnoreCase("px")
                                                        ? new Date(System.currentTimeMillis() + Long.parseLong(aa.get(i + 4)))
                                                        : new Date(System.currentTimeMillis() + Long.parseLong(aa.get(i + 4)) * 1000);
                                                for (Map.Entry<String, Map<String, Boolean>> entry : watchMap.entrySet()) {
                                                    if (entry.getValue().containsKey(aa.get(i + 1))) {
                                                        entry.getValue().put(aa.get(i + 1), true);
                                                    }
                                                }
                                                f = true;
                                                mapTime.put(aa.get(i + 1), date);

                                                String s = "*5\r\n$3\r\nSET\r\n$" + aa.get(i + 1).length() + "\r\n" + aa.get(i + 1)
                                                        + "\r\n$" + aa.get(i + 2).length() + "\r\n" + aa.get(i + 2)
                                                        + "\r\n$" + aa.get(i + 3).length() + "\r\n" + aa.get(i + 3)
                                                        + "\r\n$" + aa.get(i + 4).length() + "\r\n" + aa.get(i + 4) + "\r\n";
                                                System.out.println("DEBUG: Sending SET command 4: " + s.replace("\r\n", "\\r\\n"));

                                                byte[] commandBytes = s.getBytes(StandardCharsets.ISO_8859_1);
                                                commandEndOffset.set(lastOffset.addAndGet(commandBytes.length));

                                                for (Map.Entry<Socket, LinkedBlockingQueue<String>> entry : clientMap.entrySet()) {
                                                    entry.getValue().add(s);
                                                }
                                            }
                                            if (!f) {
                                                for (Map.Entry<String, Map<String, Boolean>> entry : watchMap.entrySet()) {
                                                    if (entry.getValue().containsKey(aa.get(i + 1))) {
                                                        entry.getValue().put(aa.get(i + 1), true);
                                                    }
                                                }
                                            }
                                            map.put(aa.get(i + 1), aa.get(i + 2));
                                            if (!f) {
                                                String s = "*3\r\n$3\r\nSET\r\n$" + aa.get(i + 1).length() + "\r\n" + aa.get(i + 1)
                                                        + "\r\n$" + aa.get(i + 2).length() + "\r\n" + aa.get(i + 2) + "\r\n";
                                                System.out.println("DEBUG: Sending SET command: " + s.replace("\r\n", "\\r\\n"));

                                                byte[] commandBytes = s.getBytes(StandardCharsets.ISO_8859_1);
                                                commandEndOffset.set(lastOffset.addAndGet(commandBytes.length));

                                                for (Map.Entry<Socket, LinkedBlockingQueue<String>> entry : clientMap.entrySet()) {
                                                    entry.getValue().add(s);
                                                }
                                            }

                                            printWriter.print("+OK" + "\r\n");
                                            printWriter.flush();
                                        } else if (aa.get(i).equals("GET")) {
                                            if (isReplica.get()) {
                                                if (replMap.containsKey(aa.get(i + 1)) && !replMapTime.containsKey(aa.get(i + 1))) {
                                                    printWriter.print("$" + replMap.get(aa.get(i + 1)).length() + "\r\n" + replMap.get(aa.get(i + 1)) + "\r\n");
                                                    printWriter.flush();
                                                } else if (replMap.containsKey(aa.get(i + 1)) && replMapTime.containsKey(aa.get(i + 1))) {
                                                    if (replMapTime.get(aa.get(i + 1)).before(new Date())) {
                                                        printWriter.print("$-1\r\n");
                                                        printWriter.flush();
                                                        replMap.remove(aa.get(i + 1));
                                                        replMapTime.remove(aa.get(i + 1));
                                                    } else {
                                                        printWriter.print("$" + replMap.get(aa.get(i + 1)).length() + "\r\n" + replMap.get(aa.get(i + 1)) + "\r\n");
                                                        printWriter.flush();
                                                    }
                                                } else {
                                                    printWriter.print("$-1\r\n");
                                                    printWriter.flush();
                                                }

                                                continue;
                                            }
                                            Queue<String> que = multiMap.get(Thread.currentThread().getName());
                                            if (que != null) {
                                                printWriter.print("+QUEUED\r\n");
                                                printWriter.flush();
                                                que.add(aa.get(i) + " " + aa.get(i + 1));
                                                continue;
                                            }
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
                                                    printWriter.flush();
                                                } else {
                                                    CopyOnWriteArrayList<String> tmpList = mapList.get(key);
                                                    String s = tmpList.removeFirst();
                                                    printWriter.print("*2\r\n");
                                                    printWriter.print("$" + key.length() + "\r\n" + key + "\r\n");
                                                    printWriter.print("$" + s.length() + "\r\n" + s + "\r\n");
                                                    printWriter.flush();
                                                }
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
                                            Queue<String> que = multiMap.get(Thread.currentThread().getName());
                                            if (que != null) {
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
                                            Queue<String> que = new LinkedList<>();
                                            multiMap.put(Thread.currentThread().getName(), que);
                                            printWriter.print("+OK\r\n");
                                            printWriter.flush();
                                        } else if (aa.get(i).equals("EXEC")) {
                                            Queue<String> que = multiMap.get(Thread.currentThread().getName());
                                            if (que != null) {
                                                if (que.isEmpty()) {
                                                    printWriter.print("*0\r\n");
                                                    printWriter.flush();
                                                    multiMap.remove(Thread.currentThread().getName());
                                                    continue;
                                                }

                                                Map<String, Boolean> watchedKeys = watchMap.getOrDefault(Thread.currentThread().getName(), null);
                                                boolean isWatched = watchedKeys != null;

                                                if (isWatched) {
                                                    boolean dontDo = false;
                                                    for (Map.Entry<String, Boolean> k : watchedKeys.entrySet()) {
                                                        if (k.getValue()) {
                                                            dontDo = true;
                                                            break;
                                                        }
                                                    }

                                                    if (dontDo) {
                                                        printWriter.print("*-1\r\n");
                                                        printWriter.flush();
                                                        multiMap.remove(Thread.currentThread().getName());
                                                        watchMap.remove(Thread.currentThread().getName());
                                                        continue;
                                                    }
                                                }

                                                StringBuffer sb = new StringBuffer();
                                                sb.append("*" + que.size() + "\r\n");
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
                                                        sb.append("+OK" + "\r\n");
                                                    } else if (task[0].equals("INCR")) {
                                                        String key = task[1];
                                                        if (map.containsKey(key)) {
                                                            int val = 0;
                                                            try {
                                                                val = Integer.parseInt(map.get(key));
                                                            } catch (NumberFormatException e) {
                                                                sb.append("-ERR value is not an integer or out of range\r\n");
                                                                continue;
                                                            }
                                                            map.put(key, String.valueOf(val + 1));
                                                            sb.append(":" + (val + 1) + "\r\n");
                                                        } else {
                                                            map.put(key, "1");
                                                            sb.append(":1\r\n");
                                                        }
                                                    } else if (task[0].equals("GET")) {
                                                        if (map.containsKey(task[1]) && !mapTime.containsKey(task[1])) {
                                                            sb.append("$" + map.get(task[1]).length() + "\r\n" + map.get(task[1]) + "\r\n");
                                                        } else if (map.containsKey(task[1]) && mapTime.containsKey(task[1])) {
                                                            if (mapTime.get(task[1]).before(new Date())) {
                                                                sb.append("$-1\r\n");
                                                                map.remove(task[1]);
                                                                mapTime.remove(task[1]);
                                                            } else {
                                                                sb.append("$" + map.get(task[1]).length() + "\r\n" + map.get(task[1]) + "\r\n");
                                                            }
                                                        } else {
                                                            sb.append("$-1\r\n");
                                                        }
                                                    }
                                                }
                                                printWriter.print(sb);
                                                printWriter.flush();
                                                multiMap.remove(Thread.currentThread().getName());
                                            } else {
                                                printWriter.print("-ERR EXEC without MULTI\r\n");
                                                printWriter.flush();
                                            }
                                        } else if (aa.get(i).equals("DISCARD")) {
                                            Queue<String> que = multiMap.get(Thread.currentThread().getName());
                                            if (que != null) {
                                                printWriter.print("+OK\r\n");
                                                multiMap.remove(Thread.currentThread().getName());
                                                que.clear();
                                            } else {
                                                printWriter.print("-ERR DISCARD without MULTI\r\n");
                                            }
                                            watchMap.remove(Thread.currentThread().getName());
                                            printWriter.flush();
                                        } else if (aa.get(i).equals("WATCH")) {
                                            Queue<String> que = multiMap.get(Thread.currentThread().getName());
                                            if (que != null) {
                                                printWriter.print("-ERR WATCH inside MULTI is not allowed\r\n");
                                            } else {
                                                List<String> keys = new ArrayList<>();
                                                for (int a = 0; a < length - 1; a++) {
                                                    keys.add(aa.get(i + a + 1));
                                                }
                                                Map<String, Boolean> keyMod = new HashMap<>();
                                                for (String k : keys) {
                                                    keyMod.put(k, false);
                                                }
                                                watchMap.put(Thread.currentThread().getName(), keyMod);

                                                printWriter.print("+OK\r\n");
                                            }
                                            printWriter.flush();
                                        } else if (aa.get(i).equals("UNWATCH")) {
                                            watchMap.remove(Thread.currentThread().getName());
                                            printWriter.print("+OK\r\n");
                                            printWriter.flush();
                                        } else if (aa.get(i).equals("INFO")) {
                                            StringBuilder infoBuilder = new StringBuilder();
                                            if (argsMap.containsKey("replicaof")) {
                                                infoBuilder.append("role:slave");
                                            } else {
                                                infoBuilder.append("role:master\r\n");
                                                infoBuilder.append("master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\n");
                                                infoBuilder.append("master_repl_offset:0");
                                            }
                                            String info = infoBuilder.toString();
                                            printWriter.print("$" + info.length() + "\r\n" + info + "\r\n");
                                            printWriter.flush();
                                        } else if (aa.get(i).equals("REPLCONF")) {
                                            if (i + 2 < aa.size() && "ack".equalsIgnoreCase(aa.get(i + 1))) {
                                                replAckMap.put(clientSocket.getRemoteSocketAddress().toString(), Long.valueOf(aa.get(i + 2)));
                                                // ack 类型不需要回复
                                                continue;
                                            }
                                            if (isReplicaCli.get() && clientMap.containsKey(clientSocket)) {
                                                clientMap.get(clientSocket).add("+OK\r\n"); // 交给专用写线
                                            } else {
                                                printWriter.print("+OK\r\n");
                                                printWriter.flush();
                                            }
                                        } else if (aa.get(i).equals("PSYNC")) {
                                            isReplicaCli.set(true);
                                            printWriter.print("+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n");
                                            printWriter.flush();
                                            byte[] bytes = hexStringToByteArray("524544495330303131fe00ff0000000000000000");
                                            clientSocket.getOutputStream().write(("$" + bytes.length + "\r\n").getBytes());
                                            clientSocket.getOutputStream().write(bytes);
                                            clientSocket.getOutputStream().flush();

                                            replicaCount.incrementAndGet();
                                            clientMap.put(clientSocket, new LinkedBlockingQueue<>());

                                            new Thread(() -> {
                                                while (true) {
                                                    try {
                                                        String task = clientMap.get(clientSocket).take();
                                                        printWriter.write(task);
                                                        printWriter.flush(); // Explicitly flush here

                                                    } catch (Exception e) {
                                                        System.out.println(e.getMessage());
                                                    }
                                                }
                                            }).start();
                                        } else if (aa.get(i).equals("WAIT")) {
                                            if (lastOffset.get() == 0) {
                                                printWriter.print(":" + replicaCount.get() + "\r\n");
                                                printWriter.flush();
                                                continue;
                                            }

                                            // **清空旧的 ACK 记录**
                                            replAckMap.clear();

                                            int required = Integer.parseInt(aa.get(i + 1));
                                            long timeoutMs = (long) (Double.parseDouble(aa.get(i + 2)));
                                            int replicaNum = clientMap.size();

                                            // 向所有副本发送 REPLCONF GETACK *
                                            String getackCommand = "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n";
                                            // 同步发送，确保所有副本都收到
                                            synchronized (clientMap) {
                                                for (Map.Entry<Socket, LinkedBlockingQueue<String>> entry : clientMap.entrySet()) {
                                                    entry.getValue().add(getackCommand);
                                                }
                                            }

                                            // 等待副本回复
                                            long start = System.currentTimeMillis();
                                            int confirmed = 0;
                                            int expectedReplies = replicaNum;

                                            while (System.currentTimeMillis() - start < timeoutMs) {
                                                confirmed = 0;
                                                for (Map.Entry<String, Long> e : replAckMap.entrySet()) {
                                                    if (e.getValue() >= commandEndOffset.get()) {
                                                        confirmed++;
                                                    }
                                                }
                                                if (confirmed >= required) break;

                                                if (replAckMap.size() >= expectedReplies) break;

                                                try {
                                                    Thread.sleep(10);
                                                } catch (InterruptedException ex) {
                                                    Thread.currentThread().interrupt();
                                                    break;
                                                }
                                            }

                                            System.out.println("DEBUG: WAIT result - required: " + required + ", confirmed: " + confirmed +
                                                    ", replAckMap size: " + replAckMap.size());
                                            printWriter.print(":" + confirmed + "\r\n");
                                            printWriter.flush();
                                        } else if (aa.get(i).equalsIgnoreCase("CONFIG")) {
                                            if (i + 2 < aa.size() && aa.get(i + 1).equalsIgnoreCase("GET")) {
                                                String name = aa.get(i + 2);
                                                Object val = argsMap.get(name);
                                                System.out.println("DEBUG: CONFIG GET - name: " + name + ", val: " + val);
                                                if (val != null) {
                                                    String response = "*2\r\n" +
                                                            "$" + name.length() + "\r\n" + name + "\r\n" +
                                                            "$" + String.valueOf(val).length() + "\r\n" + val + "\r\n";
                                                    printWriter.write(response);
                                                    printWriter.flush();
                                                } else {
                                                    printWriter.write("*0\r\n");
                                                    printWriter.flush();
                                                }
                                                // 处理完了显示跳出 避免第二个参数 GET 当成 command
                                                break;
                                            }
                                        } else if (aa.get(i).equalsIgnoreCase("KEYS")) {
                                            String pattern = aa.get(i + 1).substring(0, aa.get(i + 1).indexOf("*"));
                                            StringBuilder sb = new StringBuilder();
                                            int n = 0;
                                            for (String key : map.keySet()) {
                                                if (pattern.isEmpty() || key.startsWith(pattern)) {
                                                    sb.append("$").append(key.length()).append("\r\n").append(key).append("\r\n");
                                                    n++;
                                                }
                                            }
                                            sb.insert(0, "*" + n + "\r\n");
                                            printWriter.write(sb.toString());
                                            printWriter.flush();
                                        } else if (aa.get(i).equalsIgnoreCase("SUBSCRIBE")) {
                                            String channelName = aa.get(i + 1);
                                            CopyOnWriteArraySet<String> subChannelNames = subMap.computeIfAbsent(Thread.currentThread().getName(), k -> new CopyOnWriteArraySet<>());
                                            subChannelNames.add(channelName);
                                            isSubMod.set(true);
                                            printWriter.print("*3\r\n" +
                                                    "$9\r\nsubscribe\r\n" +
                                                    "$" + channelName.length() + "\r\n" + channelName + "\r\n" +
                                                    ":" + subChannelNames.size() + "\r\n");
                                            printWriter.flush();
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
                        System.out.println("Exception: " + e.getLocalizedMessage());
                    } finally {
                    }


                });
                client.start();
            }


        } catch (IOException e) {
            System.out.println("serverSocket IOException: " + e.getLocalizedMessage());
        } finally {
            if (serverSocket != null) {
                try {
                    serverSocket.close();
                } catch (IOException e) {
                    System.out.println("serverSocket close IOException: " + e.getLocalizedMessage());
                }
            }
        }
    }

    // Read a line (bytes) from InputStream until CRLF. Returns ISO-8859-1 String (without CRLF).
    private static String readLineFromStream(InputStream in) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        int b;
        while ((b = in.read()) != -1) {
            if (b == '\r') {
                int n = in.read();
                if (n == -1) break;
                if (n == '\n') break;
                // not LF, write CR and the read byte into buffer
                buf.write('\r');
                buf.write(n);
                continue;
            }
            buf.write(b);
        }
        if (b == -1 && buf.size() == 0) return null;
        return buf.toString("ISO-8859-1");
    }

    public static byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                    + Character.digit(s.charAt(i+1), 16));
        }
        return data;
    }

    private static String readBuf(ByteBuffer buf) {
        byte firstByte = buf.get();
        int type = (firstByte & 0xFF) >> 6;
        int len;

        switch (type) {
            case 0:  // 6-bit length
                len = firstByte & 0x3F;
                break;
            case 1:  // 14-bit length
                len = ((firstByte & 0x3F) << 8) | (buf.get() & 0xFF);
                break;
            case 2:  // 32-bit length
                len = buf.getInt();
                break;
            case 3:  // Special encoding
                int encoding = firstByte & 0x3F;
                switch (encoding) {
                    case 0:  // 8-bit signed integer
                        return String.valueOf(buf.get());
                    case 1:  // 16-bit signed integer
                        return String.valueOf(buf.getShort());
                    case 2:  // 32-bit signed integer
                        return String.valueOf(buf.getInt());
                    case 3:  // 64-bit signed integer
                        return String.valueOf(buf.getLong());
                    case 4:  // LZF compressed string
                        int compressedLen = readLenAsInt(buf);
                        int uncompressedLen = readLenAsInt(buf);
                        byte[] compressed = new byte[compressedLen];
                        buf.get(compressed);
                        byte[] uncompressed = decompressLZF(compressed, uncompressedLen);
                        return new String(uncompressed, StandardCharsets.UTF_8);
                    default:
                        throw new IllegalArgumentException("Unknown encoding: " + encoding);
                }
            default:
                throw new IllegalArgumentException("Unknown encoding type: " + type);
        }

        byte[] data = new byte[len];
        buf.get(data);
        return new String(data, StandardCharsets.UTF_8);
    }

    // 辅助方法：读取 length 编码的整数
    private static int readLenAsInt(ByteBuffer buf) {
        byte firstByte = buf.get();
        int type = (firstByte & 0xFF) >> 6;

        switch (type) {
            case 0:
                return firstByte & 0x3F;
            case 1:
                return ((firstByte & 0x3F) << 8) | (buf.get() & 0xFF);
            case 2:
                return buf.getInt();
            default:
                throw new IllegalArgumentException("Invalid length encoding");
        }
    }

    // LZF 解压（你需要找一个 LZF 库或实现）
    private static byte[] decompressLZF(byte[] compressed, int uncompressedLen) {
        // 如果你没有 LZF 库，可以使用开源实现
        // 或者返回压缩后的数据
        return compressed;  // 简单处理，实际需要真正解压
    }
}