package cis5550.webserver;

import java.net.SocketAddress;
import java.sql.Time;
import java.util.HashMap;
import java.util.Random;

public class SessionImpl implements Session{
    String m_id;
    long m_creationTime;
    long m_lastAccessedTime;
    int m_maxActiveInterval = 300;
    boolean isInvalid = false;
    boolean couldRead = true;
    SocketAddress originalAddr = null;
    private HashMap<String, Object> args = new HashMap<>();

    public static String generateId(){
        String str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        Random randomEngine = new Random();
        StringBuffer buffer = new StringBuffer();
        for(int i=0;i<25;i++){
            buffer.append(str.charAt(randomEngine.nextInt(62)));
        }
        return buffer.toString();
    }

    SessionImpl()
    {
        m_id = generateId();
        m_creationTime = System.currentTimeMillis();
        m_lastAccessedTime = m_creationTime;
    }

    @Override
    public String id() {
        checkValidity();
        updateLastAccessTime();
        return m_id;
    }

    @Override
    public long creationTime() {
        if(couldRead)
        checkValidity();
        updateLastAccessTime();
        return m_creationTime;
    }

    @Override
    public long lastAccessedTime() {
        checkValidity();
        updateLastAccessTime();
        return m_lastAccessedTime;
    }

    @Override
    public void maxActiveInterval(int seconds) {
        checkValidity();
        m_maxActiveInterval = seconds;
        updateLastAccessTime();
    }

    @Override
    public void invalidate() {
        isInvalid = true;
    }

    @Override
    public Object attribute(String name) {
        if(couldRead)
        {
            checkValidity();
            updateLastAccessTime();
            return args.get(name);
        }
        return "";
    }

    @Override
    public void attribute(String name, Object value) {
        checkValidity();
        args.put(name.toLowerCase(),value);
        updateLastAccessTime();
    }

    void updateLastAccessTime()
    {
        m_lastAccessedTime = System.currentTimeMillis();
    }

    void checkValidity()
    {
        if((System.currentTimeMillis()-m_lastAccessedTime)
            > m_maxActiveInterval*1000)
        {
            invalidate();
        }
    }
}
