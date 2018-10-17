import com.visuality.nq.auth.Credentials;
import com.visuality.nq.auth.PasswordCredentials;
import com.visuality.nq.client.*;
import com.visuality.nq.common.*;
import com.visuality.nq.config.Config;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.*;

public class Main {
    private static String stringTestContent = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum. Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum. Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut eni\r\n";
    private static byte[] testContent = stringTestContent.getBytes(StandardCharsets.UTF_8);

    private static final String SMBTIMEOUT = "SMBTIMEOUT";
    private static final String LOGTOFILE = "LOGTOFILE";
    private static final String ENABLECAPTUREPACKETS = "ENABLECAPTUREPACKETS";
    private static final String LOGTHRESHOLD = "LOGTHRESHOLD";

    private static final String[] integerPropertiesArray = {"INTERNALNAMESERVICEPORT", "INTERNALDATAGRAMSERVICEPORT",
            "MAXSECURITYLEVEL", "TICKETTTL", "DFSCACHETTL", "RETRYCOUNT", "RETRYTIMEOUT", "LOGTHRESHOLD",
            "LOGMAXRECORDSINFILE", "LOGMAXFILES", "CAPTUREMAXRECORDSINFILE", "CAPTUREMAXFILES"};
    private static final HashSet<String> integerProperties = new HashSet<>(Arrays.asList(integerPropertiesArray));

    private static final String[] booleanPropertiesArray = {"ISUSEINTERNALONLY", "ISUSEEXTERNALONLY",
            "BINDWELLKNOWNPORTS", "ENABLENONSECUREAUTHMETHODS", "SIGNINGPOLICY", "USECACHE", "STOREKEY", "DFSENABLE",
            "LOGTOFILE", "LOGTOCONSOLE", "ENABLECAPTUREPACKETS"};
    private static final HashSet<String> booleanProperties = new HashSet<>(Arrays.asList(booleanPropertiesArray));

    private static boolean logToFile = false;
    private static boolean enableCapturePackets = false;

    public static void main(String[] args) throws NqException {
        if (args.length < 3) {
            System.out.println("jnqTest.jar <serverAndShare> <domainAndUsername> <password> <configFilename>");
            return;
        }

        final String serverAndShare = args[0];
        final String domainAndUsername = args[1];
        final String password = args[2];
        final String configFilename;

        if (args.length > 3) {
            configFilename = args[3];
        } else {
            configFilename = null;
        }

        System.out.printf("jNQ jar: %s%n", "jNQ-1.0.2.br-1.3.jar");
        final JnqVersion jnqVersion = new JnqVersion();
        System.out.printf("jNQ version: %s%n", jnqVersion.getJnqVersion());
        System.out.printf("jNQ build time: %s%n%n", jnqVersion.getJnqBuildTime());

        System.out.printf("Command line parameters: '%s', '%s', '%s', '%s'%n", serverAndShare, domainAndUsername, password, configFilename);

        setConfigs(configFilename);
        handleStartLogging();
        printShares(getServer(serverAndShare));
        Mount mount = null;
        try {
            mount = mount(serverAndShare, domainAndUsername, password);

//          writeRandomData(mount, "jNQTest_0.txt", 0);
//          writeRandomData(mount, "jNQTest_1.txt", 1);
//          writeRandomData(mount, "jNQTest_2.txt", 2);
//          writeRandomData(mount, "jNQTest_3.txt", 3);
//          writeRandomData(mount, "jNQTest_1023.txt", 1023);
//          writeRandomData(mount, "jNQTest_1024.txt", 1024);
//          writeRandomData(mount, "jNQTest_1025.txt", 1025);
//          writeRandomData(mount, "jNQTest_2047.txt", 2047);
//          writeRandomData(mount, "jNQTest_2048.txt", 2048);
//          writeRandomData(mount, "jNQTest_2049.txt", 2049);
//          writeRandomData(mount, "jNQTest_1MB.txt", 1024 * 1024); // 1 MB
//          writeRandomData(mount, "jNQTest_1GB.txt", 1024 * 1024 * 1024); // 1 GB
        } finally {
            unmount(mount);

            Capture.stop();
            TraceLog.get().stop();
            Client.stop(); // Need to stop all jNQ threads, program won't finish without it;
            printLog("Client.stop() called");
            printLog("Process waiting to finish...");
        }
    }

    private static void setConfigs(String configFilename) {
        if (configFilename == null || configFilename.length() == 0) {
            return;
        }
        System.out.println();
        printLog(String.format("Applying settings from '%s'", configFilename));
        try (InputStream input = new FileInputStream(configFilename)) {
            Properties prop = new Properties();
            prop.load(input);
            List<String> keyList = new ArrayList<>(prop.stringPropertyNames());
            Collections.sort(keyList);
            for (String key : keyList) {
                setConfig(key, prop.getProperty(key));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println();
    }

    private static void setConfig(String key, String value) {
        System.out.printf("%s: %s%n", key, value);

        try {
            if (key.startsWith("DIALECT_")) {
                setDialect(key, value);
                return;
            }

            if (SMBTIMEOUT.equals(key)) {
                final long timeout = Long.valueOf(value);
                if (timeout >= 0) {
                    Client.setSmbTimeout(timeout);
                } else {
                    throw new NqException("illegal SMBTIMEOUT value");
                }
                return;
            }

            if (value == null) {
                Config.jnq.set(key, null);
                return;
            }

            if (booleanProperties.contains(key)) {
                if ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
                    boolean booleanValue = Boolean.valueOf(value);

                    if (ENABLECAPTUREPACKETS.equals(key)) {
                        enableCapturePackets = booleanValue;
                        return;
                    }
                    if (LOGTOFILE.equals(key)) {
                        logToFile = booleanValue;
                        return;
                    }

                    Config.jnq.set(key, booleanValue);
                    return;
                } else {
                    throw new NqException("Setting \"" + key + "\" must be set to \"true\" or \"false\"");
                }
            }

            if (integerProperties.contains(key)) {
                final int intValue = Integer.valueOf(value);
                Config.jnq.set(key, intValue);
                if (LOGTHRESHOLD.equals(key)) {
                    TraceLog.get().setThreshold(intValue);
                }
                return;
            }

            Config.jnq.set(key, value);
        } catch (NqException e) {
            e.printStackTrace();
        }
    }

    private static void setDialect(String dialect, String enable) throws NqException {
        System.out.printf("%s: %s%n", dialect, enable);

        short dialectId;
        switch (dialect) {
            case "DIALECT_100":
                dialectId = Smb.DIALECT_100;
                break;
            case "DIALECT_202":
                dialectId = Smb.DIALECT_202;
                break;
            case "DIALECT_210":
                dialectId = Smb.DIALECT_210;
                break;
            case "DIALECT_300":
                dialectId = Smb.DIALECT_300;
                break;
            case "DIALECT_302":
                dialectId = Smb.DIALECT_302;
                break;
            case "DIALECT_311":
                dialectId = Smb.DIALECT_311;
                break;
            default:
                throw new IllegalArgumentException("Invalid dialect: " + dialect);
        }

        Client.setDialect(dialectId, Boolean.valueOf(enable));
    }

    private static void handleStartLogging() throws NqException {
        if (logToFile) {
            Config.jnq.set(LOGTOFILE, true);
            TraceLog.get().start();
            printLog("TraceLog.get().start() called");
        } else {
            Config.jnq.set(LOGTOFILE, false);
        }
        if (enableCapturePackets) {
            Config.jnq.set(ENABLECAPTUREPACKETS, true);
            Capture.start();
            printLog("Capture.start() called");
        } else {
            Config.jnq.set(ENABLECAPTUREPACKETS, false);
        }
    }

    private static void printShares(String server) {
        printLog("Getting shares");
        try {
            System.out.printf("Shares on %s: ", server);
            Iterator shares = Network.enumerateShares(server);
            while (shares.hasNext()) {
                Share.Info shareInfo = (Share.Info) shares.next();
                System.out.print(shareInfo.name);
                if (shareInfo.comment != null && shareInfo.comment.length() > 0) {
                    System.out.printf(" (%s)", shareInfo.comment);
                }
                if (shares.hasNext()) {
                    System.out.print(", ");
                } else {
                    System.out.print(".");
                }
            }
        } catch (NqException e) {
            e.printStackTrace();
        }
        System.out.println();
        printLog("Getting shares done");
    }

    private static Mount mount(String serverAndShare, String domainAndUsername, String password) {
        printLog("Mount Start");

        Mount mount = null;
        try {
            Credentials credentials = new PasswordCredentials(
                    getUsername(domainAndUsername),
                    password,
                    getDomain(domainAndUsername)
            );

            mount = new Mount(getServer(serverAndShare), getShare(serverAndShare), credentials);
            printLog("Mount Done");
            Socket socket = mount.getShare().getUser().getServer().getTransport().getSocket();
            System.out.printf(
                    "Successful Mount: host=%s, address=%s, port=%s, dialect=%s, shareName=%s, retryCount=%s, retryTimeout=%s, DFSENABLE=%s.%n",
                    socket.getInetAddress().getHostName(), socket.getInetAddress().getHostAddress(),
                    socket.getPort(), mount.getInfo().getDialectVersion(), mount.getShareName(), mount.getRetryCount(),
                    mount.getRetryTimeout(), Config.jnq.getBool("DFSENABLE"));
        } catch (NqException e) {
            e.printStackTrace();
            printLog("Mount Exception");
            unmount(mount);
            mount = null;
        }

        return mount;
    }

    private static void unmount(final Mount mount) {
        if (mount != null) {
            mount.close();
            printLog("Mount Closed");
        }
    }

    private static void writeRandomData(final Mount mount, final String remotefile, final long targetLength) {
        // An existing remotefile will be overwritten.

        if (mount == null) {
            return;
        }

        byte[] content = getRandomByteArrayof1MB();

        try (SmbOutputStream os = new SmbOutputStream(mount, remotefile)) {
            System.out.printf("Writing '%s': ", remotefile);
            int lastPrintedPercentage = 0;
            long writtenLength = 0;
            while (writtenLength < targetLength) {
                final int writeLength;
                if (writtenLength + content.length > targetLength) {
                    writeLength = (int) (targetLength - writtenLength);
                } else {
                    writeLength = content.length;
                }

                os.write(content, 0, writeLength);
                writtenLength += writeLength;

                final int percentage = (int) (writtenLength * 100 / targetLength);
                if (percentage != lastPrintedPercentage) {
                    System.out.printf("%,d%% ", percentage);
                    lastPrintedPercentage = percentage;
                }
            }
            System.out.printf("%n");

            System.out.printf("Successfully written random data to remotefile='%s' (%,d bytes).%n", remotefile, writtenLength);
        } catch (NqException e) {
            System.out.printf("%n");
            e.printStackTrace();
            System.out.printf("Exception writing random data to remotefile='%s'.%n", remotefile);
        } catch (IOException e) {
            System.out.printf("%n");
            printErrorMessage(e);

            e.printStackTrace();
            System.out.printf("Exception writing random data to remotefile='%s'.%n", remotefile);
        }
    }

    private static void printErrorMessage(IOException exception) {
        System.out.println("exception: " + exception.getClass().getName());
        final String message = exception.getMessage();
        Throwable cause = exception.getCause();
        System.out.println("cause: " + cause);
        if (cause instanceof NqException) {
            NqException nqException = (NqException) cause;
            int nqErrorCode = nqException.getErrCode();
            String nqMessage = nqException.getMessage();
            System.out.printf("Exception '%s', Cause: '%s', Error Code: %d.%n", message, nqMessage, nqErrorCode);
        } else if (cause != null) {
            String causeMessage = cause.getMessage();
            System.out.printf("Exception '%s', Cause: '%s'.%n", message, causeMessage);
        } else {
            System.out.printf("Exception '%s'.%n", message);
        }
    }

    private static byte[] getRandomByteArrayof1MB() {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        for (int i = 0; i < 1024; i++) {
            try {
                os.write(testContent);
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("Exception concatenating random data.");
            }
        }
        return os.toByteArray();
    }

    private static String getDomain(String domainAndUsername) {
        if (domainAndUsername == null) {
            return ".";
        }
        String[] domainAndUsernameArray = domainAndUsername.split("\\\\");
        if (domainAndUsernameArray.length > 1) {
            return domainAndUsernameArray[0];
        }
        return ".";
    }

    private static String getUsername(String domainAndUsername) {
        if (domainAndUsername == null) {
            return "";
        }
        String[] domainAndUsernameArray = domainAndUsername.split("\\\\");
        if (domainAndUsernameArray.length > 1) {
            return domainAndUsernameArray[1];
        }
        return domainAndUsername;
    }

    private static String getServer(String serverAndShare) {
        if (serverAndShare == null) {
            return "";
        }
        String[] serverAndShareArray = getCanonicalServerAndShare(serverAndShare).split("/");
        if (serverAndShareArray.length > 1) {
            return serverAndShareArray[0];
        }
        return serverAndShare;
    }

    private static String getShare(String serverAndShare) {
        if (serverAndShare == null) {
            return "";
        }
        String[] serverAndShareArray = getCanonicalServerAndShare(serverAndShare).split("/");
        if (serverAndShareArray.length > 1) {
            return serverAndShareArray[1];
        }
        return "";
    }

    private static String getCanonicalServerAndShare(String serverAndShare) {
        if (serverAndShare == null) {
            return "";
        }
        String canonicalServerAndShare = serverAndShare.replace('\\', '/');

        if (canonicalServerAndShare.startsWith("smb:")) {
            canonicalServerAndShare = canonicalServerAndShare.substring(4);
        }

        if (canonicalServerAndShare.startsWith("//")) {
            canonicalServerAndShare = canonicalServerAndShare.substring(2);
        }

        if (canonicalServerAndShare.endsWith("/")) {
            canonicalServerAndShare = canonicalServerAndShare.substring(0, canonicalServerAndShare.length() - 1);
        }

        return canonicalServerAndShare;
    }

    private static void printLog(String message) {
        System.out.printf("%tT:%<tL %s%n", new Timestamp(System.currentTimeMillis()), message);
    }

    private static void waitForEnter(String message) {
        Console console = System.console();
        if (console == null) {
            return;
        }
        console.printf("%nPress Enter to continue %s.%n", message);
        console.readLine();
    }
}
