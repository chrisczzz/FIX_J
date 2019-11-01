package com.hftj;

import com.lmax.api.Callback;
import com.lmax.api.FailureResponse;
import com.lmax.api.LmaxApi;
import com.lmax.api.account.*;
import quickfix.*;
import quickfix.field.*;
import quickfix.fix44.MarketDataRequest;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Hashtable;

import static quickfix.SystemTime.getLocalDateTime;



public class LMAX_App extends MessageCracker implements Application, Subject, LoginCallback, AccountStateEventListener {

    private ArrayList<Instrument> instrument_list;
    private ArrayList<Tick_Listener> tick_listeners;
    private Hashtable<String, Execution_Report_Listener> exereportHashtable;
    private Hashtable<String, Tick_Listener> tickListenerHashtable;

    private static int marketdepth = 5;

    private static volatile SessionID MsessionID;
    private static volatile SessionID TsessionID;

    private com.lmax.api.Session websession;

    private static String username = "zhipc123";
    private static String password = "Czp1991918";

    public LMAX_App() throws ConfigError {
        instrument_list = new ArrayList<Instrument>();
        exereportHashtable = new Hashtable<>();
        tickListenerHashtable = new Hashtable<>();
    }

    @Override
    public void register(TickStrategy tickStrategy) {
        instrument_list.add(tickStrategy.instrument);
        exereportHashtable.put(tickStrategy.instrument.SecurityID, tickStrategy);
        tickListenerHashtable.put(tickStrategy.instrument.SecurityID, tickStrategy);
    }

    @Override
    public void unregister(TickStrategy tickStrategy) {
        instrument_list.remove(tickStrategy.instrument);
        exereportHashtable.remove(tickStrategy.instrument.SecurityID);
        tickListenerHashtable.remove(tickStrategy.instrument.SecurityID);
    }

    public void Notify_Tick_Listener(String Secid, double[] market_entry) {
        new Thread(new Runnable() {
            public void run() {
            tickListenerHashtable.get(Secid).onTick(market_entry);
            }
        }).start();
        }

    public void Notify_exe_Listener(quickfix.fix44.ExecutionReport message) {
        new Thread(new Runnable() {
            public void run() {
                try {
                    String Secid = message.getSecurityID().getValue();
                    exereportHashtable.get(Secid).onExecutionReport(message);
                } catch (FieldNotFound | SessionNotFound fieldNotFound) {
                    fieldNotFound.printStackTrace();
                }
            }
        }).start();
    }

    @Override
    public void onLoginSuccess(com.lmax.api.Session session) {
        System.out.println("Logged on web trading platform." + "\nAccount details: "
                + session.getAccountDetails());
        this.websession = session;
        session.registerAccountStateEventListener(this);
        session.subscribe(new AccountSubscriptionRequest(), new Callback()
        {
            public void onSuccess()
            {
                System.out.println("Successful subscription");
            }

            public void onFailure(FailureResponse failureResponse)
            {
                System.err.printf("Failed to subscribe: %s%n", failureResponse);
            }
        });

        session.start();
    }

    @Override
    public void onLoginFailure(FailureResponse failureResponse) {
        System.out.println("Failed to log on to web trading platform: "
                + failureResponse);
    }


    @Override
    public void notify(AccountStateEvent accountStateEvent) {
        System.out.println("Testing account state event: "+ accountStateEvent.getBalance());
    }

    @Override
    public void onCreate(SessionID sessionID) {}

    @Override
    public void onLogon(SessionID sessionID) {
        System.out.println("OnLogon");
        if (sessionID.getTargetCompID().equals("LMXBD")){
            System.out.println("logged on trading session");
            TsessionID = sessionID;
        }else{
            MsessionID = sessionID;
            subscribe_market_data(instrument_list, MsessionID);
        }
    }

    @Override
    public void onLogout(SessionID sessionID) {
        System.out.println("OnLogout");
        MsessionID = null;
        TsessionID = null;
    }

    @Override
    public void toAdmin(Message message, SessionID sessionID) {
        if (message instanceof quickfix.fix44.Logon) {
            System.out.println("Attempt to log on...");
            try {
                message.setString(quickfix.field.Username.FIELD, username);
                message.setString(quickfix.field.Password.FIELD, password);
            }
            catch (Exception ex) {
                throw new RuntimeException();
            }
        }
    }

    @Override
    public void fromAdmin(Message message, SessionID sessionID) throws FieldNotFound, IncorrectDataFormat, IncorrectTagValue, RejectLogon {}

    @Override
    public void toApp(Message message, SessionID sessionID) throws DoNotSend {}

    @Override
    public void fromApp(Message message, SessionID sessionID) throws FieldNotFound, IncorrectDataFormat, IncorrectTagValue, UnsupportedMessageType {
        try {
            crack(message, sessionID);
        } catch (UnsupportedMessageType unsupportedMessageType) {
            unsupportedMessageType.printStackTrace();
        }
    }

    public void onMessage(quickfix.fix44.MarketDataSnapshotFullRefresh  message, SessionID sessionID) throws FieldNotFound {

        double[] market_entry = new double[marketdepth*4 +1];

        quickfix.fix44.MarketDataSnapshotFullRefresh.NoMDEntries group =
                new quickfix.fix44.MarketDataSnapshotFullRefresh.NoMDEntries();
        MDEntryPx MDEntryPx = new MDEntryPx();
        MDEntrySize MDEntrySize = new MDEntrySize();
        MDEntryDate date = new MDEntryDate();
        MDEntryTime time = new MDEntryTime();

        for (int i = 0; i< marketdepth*2; i++){
            message.getGroup(i+1, group);
            market_entry[i*2 + 1] = group.get(MDEntryPx).getValue();
            market_entry[i*2 + 2] = group.get(MDEntrySize).getValue();

            if (i+1 == 1){
                LocalDateTime dt = LocalDateTime.of(group.get(date).getValue(), group.get(time).getValue());
                market_entry[0] = dt.toInstant(ZoneOffset.UTC).toEpochMilli();
            }
        }
        Notify_Tick_Listener(message.getSecurityID().getValue(), market_entry);
    }

    public void onMessage(quickfix.fix44.ExecutionReport message, SessionID sessionID) throws FieldNotFound {
        System.out.println("catching execution report");
        System.out.print(message);
        Notify_exe_Listener(message);
    }

    public void onMessage(quickfix.fix44.OrderCancelReject message, SessionID sessionID) throws FieldNotFound {
        System.out.println("Order cancel reject");
        String ClOrdID = message.getOrigClOrdID().getValue();
        int reason = message.getCxlRejReason().getValue();

        switch (reason){
            case 1: System.out.println("Order ID: " + ClOrdID + "\nCancel reason: reason unknown");
                break;

            case 2: System.out.println("Order ID: " + ClOrdID + "\nCancel reason: Broker/Exchange Option");
                break;

            case 3: System.out.println("Order ID: " + ClOrdID + "\nCancel reason: Duplicate ClOrd received");
                break;
        }
    }

    private void subscribe_market_data(ArrayList<Instrument> instrument_list, SessionID sessionID){
        Session s = Session.lookupSession(sessionID);

        MarketDataRequest mkt = new MarketDataRequest();
        mkt.set(new MDReqID("10000"));
        mkt.set(new SubscriptionRequestType('1'));
        mkt.set(new MarketDepth(marketdepth));
        mkt.set(new MDUpdateType(0));
        mkt.set(new NoMDEntryTypes(2));
        //add in market data repeating group, subscribing bid ask at the same time
        quickfix.fix44.MarketDataRequest.NoMDEntryTypes mk2 = new quickfix.fix44.MarketDataRequest.NoMDEntryTypes();
        mk2.set(new MDEntryType('0'));
        mkt.addGroup(mk2);
        quickfix.fix44.MarketDataRequest.NoMDEntryTypes mk1 = new quickfix.fix44.MarketDataRequest.NoMDEntryTypes();
        mk1.set(new MDEntryType('1'));
        mkt.addGroup(mk1);

        //adding instrument repeating group
        mkt.set(new NoRelatedSym(instrument_list.size()));

        for (Instrument ins: instrument_list){
            quickfix.fix44.QuoteRequest.NoRelatedSym mk3 = new quickfix.fix44.QuoteRequest.NoRelatedSym();
            mk3.set(new SecurityID(ins.SecurityID));
            mk3.set(new SecurityIDSource("8"));
            mkt.addGroup(mk3);
        }
        s.send(mkt);
    }

    public static void main(String[] args) throws ConfigError, InterruptedException, SessionNotFound {

        LMAX_App test_app = new LMAX_App();

        SessionSettings settings = new SessionSettings("rc/config.properties");

        EURUSD_MM_L2 EURUSD_MM = new EURUSD_MM_L2(test_app);
        System.out.println(EURUSD_MM.instrument);

        test_app.register(EURUSD_MM);
        MessageStoreFactory messageStoreFactory = new FileStoreFactory(settings);
        LogFactory logFactory = new ScreenLogFactory( true, true, true);
        MessageFactory messageFactory = new DefaultMessageFactory();

        Initiator initiator = new SocketInitiator(test_app, messageStoreFactory, settings, logFactory, messageFactory);

        initiator.start();
        while ((MsessionID == null)&&(TsessionID ==null)){
            Thread.sleep(1000);
        }
    }
}
