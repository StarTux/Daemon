package com.winthier.daemon;

import com.winthier.connect.AbstractConnectHandler;
import com.winthier.connect.Client;
import com.winthier.connect.Connect;
import com.winthier.connect.ConnectHandler;
import com.winthier.connect.Message;
import com.winthier.connect.OnlinePlayer;
import com.winthier.connect.ServerConnection;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.bukkit.ChatColor;
import org.json.simple.JSONValue;
import org.json.simple.parser.ParseException;
import org.yaml.snakeyaml.Yaml;

public final class Daemon implements ConnectHandler {
    private static Daemon instance;
    private Connect connect;
    private final LinkedBlockingQueue<Runnable> tasks = new LinkedBlockingQueue<>();
    private volatile boolean shouldStop;
    private HashMap<UUID, User> users = new HashMap<>();
    private List<Server> servers = new ArrayList<>();
    private Map<UUID, Game> openGames = new HashMap<>();
    private List<Game> games = new ArrayList<>();
    private Random random = new Random(System.currentTimeMillis());
    private Map<UUID, String> playerCache = null;
    private List<WorldInfo> worldInfos = null;
    private List<PlayMode> playModes = null;
    private List<ChatColor> niceColors = Arrays.asList(ChatColor.BLUE, ChatColor.GREEN, ChatColor.GOLD, ChatColor.AQUA, ChatColor.LIGHT_PURPLE);
    boolean dirtyServers, dirtyUsers, dirtyGames;

    // Upstart

    public static void main(String[] args) throws Exception {
        instance = new Daemon();
        instance.main();
        System.exit(0);
    }

    private void main() {
        // Setup and load state
        for (int i = 0; i < 5; i += 1) {
            servers.add(new Server(i));
        }
        loadServers();
        loadUsers();
        loadGames();
        loadOpenGames();
        //
        connect = new Connect("daemon", new File("servers.txt"), this);
        connect.start();
        List<Runnable> currentTasks = new ArrayList<>();
        //
        new Thread(() -> {
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            while (true) {
                System.out.print("> ");
                String line;
                try {
                    line = reader.readLine();
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                    break;
                }
                if (line == null) break;
                if (line.isEmpty()) continue;
                final String[] toks = line.split(" ");
                if (toks.length == 0) continue;
                final String cmd = toks[0].trim();
                if (cmd.isEmpty()) continue;
                final String[] args = Arrays.copyOfRange(toks, 1, toks.length);
                final Semaphore sem = new Semaphore(0);
                tasks.add(() -> {
                        try {
                            syncConsoleCommand(cmd, args);
                        } finally {
                            sem.release();
                        }
                    });
                try {
                    sem.acquire();
                } catch (InterruptedException ie) {
                    ie.printStackTrace();
                }
            }
            shouldStop = true;
            System.out.println("Reader thread terminating");
        }).start();
        // Sync Tasks
        while (!shouldStop) {
            try {
                mainLoop();
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }
    }

    void mainLoop() {
        if (dirtyServers) saveServers();
        if (dirtyUsers) saveUsers();
        if (dirtyGames) saveOpenGames();
        dirtyServers = false;
        dirtyUsers = false;
        dirtyGames = false;
        for (Server server: servers) {
            if (server.state == Server.State.RUN) {
                if (!gameSessionExists(server.index)) {
                    if (server.currentGame != null) {
                        Game game = openGames.get(server.currentGame);
                        if (game != null) {
                            openGames.remove(game.uniqueId);
                            dirtyGames = true;
                            for (UUID member: game.members) {
                                User user = users.get(member);
                                if (user != null && game.uniqueId.equals(user.currentGame)) {
                                    users.remove(member);
                                }
                            }
                            dirtyUsers = true;
                        }
                    }
                    server.reset();
                    dirtyServers = true;
                }
            }
        }
        for (Game game: new ArrayList<>(openGames.values())) {
            for (UUID member: new ArrayList<>(game.members)) {
                User user = getUser(member);
                if (!game.uniqueId.equals(user.currentGame)) {
                    game.members.remove(member);
                    game.spectators.remove(member);
                    dirtyGames = true;
                }
            }
            if (game.members.isEmpty()) {
                openGames.remove(game.uniqueId);
                dirtyGames = true;
            }
        }
        Runnable task;
        try {
            task = tasks.poll(1, TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            return;
        }
        if (task != null) task.run();
    }

    // Connect Overrides

    @Override
    public void runThread(Runnable runnable) {
        new Thread(runnable).start();
    }

    @Override // ASYNC
    public void handleClientConnect(Client client) {
        tasks.add(() -> syncHandleClientConnect(client));
    }

    void syncHandleClientConnect(Client client) {
        Server server = null;
        for (Server serveri: servers) {
            if (client.getName().equals("game" + serveri.index)) {
                server = serveri;
                break;
            }
        }
        if (server != null && server.postBootMessages != null) {
            for (Object message: server.postBootMessages) {
                connect.send("game" + server.index, "minigames", message);
            }
            server.postBootMessages = null;
            dirtyServers = true;
        }
    }

    @Override // ASYNC
    public void handleClientDisconnect(Client client) { }

    @Override // ASYNC
    public void handleServerConnect(ServerConnection connection) { }

    @Override // ASYNC
    public void handleServerDisconnect(ServerConnection connection) { }

    @Override // ASYNC
    public void handleMessage(Message message) {
        // String pl = message.getPayload() == null ? "N/A" : message.getPayload().toString();
        // System.out.format("MSG ch=%s fr=%s to=%s pl=%s\n", message.getChannel(), message.getFrom(), message.getTo(), pl);
        if (message.getChannel().equals("minigames")) {
            tasks.add(() -> syncMinigamesMessage(message));
        }
    }

    @Override // ASYNC
    public void handleRemoteCommand(OnlinePlayer sender, String server, String[] args) {
        if (args.length == 0) return;
        switch (args[0].toLowerCase()) {
        case "game": case "games":
            tasks.add(() -> syncGameCommand(sender, server, Arrays.copyOfRange(args, 1, args.length)));
            break;
        default: break;
        }
    }

    // Users

    @RequiredArgsConstructor
    static final class User {
        private final UUID uuid;
        private String name;
        private UUID currentGame;

        @SuppressWarnings("unchecked")
        void load(Map<String, Object> map) {
            name = (String)map.get("name");
            if (map.containsKey("current_game")) currentGame = UUID.fromString((String)map.get("current_game"));
        }

        void store(Map<String, Object> map) {
            map.put("uuid", uuid.toString());
            map.put("name", name);
            if (currentGame != null) map.put("current_game", currentGame.toString());
        }
    }

    @SuppressWarnings("unchecked")
    void loadUsers() {
        users.clear();
        File file = new File("save/users.save");
        if (!file.isFile()) return;
        List<Map<String, Object> > list;
        try {
            list = (List<Map<String, Object> >)JSONValue.parseWithException(new FileReader(file));
        } catch (IOException ioe) {
            ioe.printStackTrace();
            return;
        } catch (ParseException pe) {
            pe.printStackTrace();
            return;
        }
        if (list == null) return;
        for (Map<String, Object> map: list) {
            UUID uuid = UUID.fromString((String)map.get("uuid"));
            User user = users.get(uuid);
            if (user == null) {
                user = new User(uuid);
                users.put(uuid, user);
            }
            user.load(map);
        }
    }

    void saveUsers() {
        List<Object> js = new ArrayList<>();
        for (User user: users.values()) {
            Map<String, Object> map = new HashMap<>();
            user.store(map);
            js.add(map);
        }
        File file = new File("save/users.save");
        try {
            FileWriter writer = new FileWriter(file);
            JSONValue.writeJSONString(js, writer);
            writer.flush();
            writer.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    User getUser(OnlinePlayer player) {
        User user = users.get(player.getUuid());
        if (user == null) {
            final UUID uuid = player.getUuid();
            user = new User(uuid);
            user.name = player.getName();
            users.put(uuid, user);
        }
        return user;
    }

    User getUser(UUID uuid) {
        User user = users.get(uuid);
        if (user == null) {
            user = new User(uuid);
            users.put(uuid, user);
        }
        return user;
    }

    // Servers

    @RequiredArgsConstructor
    static final class Server {
        enum State {
            OFF, BOOT, RUN, SHUTDOWN;
        }
        private final int index;
        private UUID currentGame;
        private State state = State.OFF;
        private List<Object> postBootMessages;

        @SuppressWarnings("unchecked")
        void load(Map<String, Object> map) {
            if (map.containsKey("current_game")) currentGame = UUID.fromString((String)map.get("current_game"));
            if (map.containsKey("state")) {
                try {
                    state = State.valueOf((String)map.get("state"));
                } catch (IllegalArgumentException iae) {
                    iae.printStackTrace();
                }
            }
            if (state == null) state = State.OFF;
            if (map.containsKey("post_boot_messages")) postBootMessages = (List<Object>)map.get("post_boot_messages");
        }

        void store(Map<String, Object> map) {
            map.put("index", index);
            if (currentGame != null) map.put("current_game", currentGame.toString());
            if (state != null) map.put("state", state.name());
            if (postBootMessages != null) map.put("post_boot_messages", postBootMessages);
        }

        void reset() {
            currentGame = null;
            state = State.OFF;
            postBootMessages = null;
        }
    }

    @SuppressWarnings("unchecked")
    void loadServers() {
        File file = new File("save/servers.save");
        if (!file.isFile()) return;
        List<Map<String, Object> > list;
        try {
            list = (List<Map<String, Object> >)JSONValue.parseWithException(new FileReader(file));
        } catch (IOException ioe) {
            ioe.printStackTrace();
            return;
        } catch (ParseException pe) {
            pe.printStackTrace();
            return;
        }
        if (list == null) return;
        for (int i = 0; i < list.size() && i < servers.size(); i += 1) {
            Map<String, Object> map = list.get(i);
            Server server = servers.get(i);
            server.load(map);
        }
    }

    void saveServers() {
        List<Object> js = new ArrayList<>();
        for (Server server: servers) {
            Map<String, Object> map = new HashMap<>();
            server.store(map);
            js.add(map);
        }
        File file = new File("save/servers.save");
        try {
            FileWriter writer = new FileWriter(file);
            JSONValue.writeJSONString(js, writer);
            writer.flush();
            writer.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    // Games

    static final class Game {
        private String name, displayName, description, shorthand;
        private int priority;
        private String setupScript;
        private int minPlayers = 1;
        private boolean connect = false; // Connect and daemon awareness
        // For created games only
        private UUID uniqueId;
        private boolean playersMayJoin = true;
        private boolean playersMaySpectate = true;
        private int serverId = -1;
        private String mapId;
        private boolean debug;
        private List<UUID> members;
        private List<UUID> spectators;
        private List<UUID> invitees;
        private UUID owner;
        private boolean publicGame;
        private String playMode;

        Game() { }

        Game(Game copy) {
            this.name = copy.name;
            this.displayName = copy.displayName;
            this.description = copy.description;
            this.shorthand = copy.shorthand;
            this.priority = copy.priority;
            this.setupScript = copy.setupScript;
            this.minPlayers = copy.minPlayers;
            this.connect = copy.connect;
            this.uniqueId = copy.uniqueId;
            this.mapId = copy.mapId;
            this.debug = copy.debug;
            if (copy.members != null) this.members = new ArrayList<>(copy.members);
            if (copy.spectators != null) this.spectators = new ArrayList<>(copy.spectators);
            if (copy.invitees != null) this.invitees = new ArrayList<>(copy.invitees);
            this.owner = copy.owner;
            this.publicGame = copy.publicGame;
            this.playersMayJoin = copy.playersMayJoin;
            this.playersMaySpectate = copy.playersMaySpectate;
            this.serverId = copy.serverId;
            this.playMode = copy.playMode;
        }

        @SuppressWarnings("unchecked")
        void load(Map<String, Object> map) {
            if (map.containsKey("name")) name = (String)map.get("name");
            if (map.containsKey("display_name")) displayName = (String)map.get("display_name");
            if (displayName == null) displayName = name;
            if (map.containsKey("description")) description = (String)map.get("description");
            if (description == null) description = displayName;
            if (map.containsKey("shorthand")) shorthand = (String)map.get("shorthand");
            if (shorthand == null) shorthand = name.substring(0, 2).toLowerCase();
            if (map.containsKey("priority")) priority = ((Number)map.get("priority")).intValue();
            if (map.containsKey("setup_script")) setupScript = (String)map.get("setup_script");
            if (map.containsKey("min_players")) minPlayers = ((Number)map.get("min_players")).intValue();
            if (map.containsKey("connect")) connect = map.get("connect") == Boolean.TRUE;
            if (setupScript == null) setupScript = "base-game.setup";
            if (map.containsKey("unique_id")) uniqueId = UUID.fromString((String)map.get("unique_id"));
            if (map.containsKey("map_id")) mapId = (String)map.get("map_id");
            if (map.containsKey("debug")) debug = map.get("debug") == Boolean.TRUE;
            if (map.containsKey("members")) members = ((List<String>)map.get("members")).stream().map(UUID::fromString).collect(Collectors.toCollection(ArrayList::new));
            if (map.containsKey("spectators")) spectators = ((List<String>)map.get("spectators")).stream().map(UUID::fromString).collect(Collectors.toCollection(ArrayList::new));
            if (map.containsKey("invitees")) invitees = ((List<String>)map.get("invitees")).stream().map(UUID::fromString).collect(Collectors.toCollection(ArrayList::new));
            if (map.containsKey("owner")) owner = UUID.fromString((String)map.get("owner"));
            if (map.containsKey("public")) publicGame = map.get("public") == Boolean.TRUE;
            if (map.containsKey("players_may_join")) playersMayJoin = map.get("players_may_join") != Boolean.FALSE;
            if (map.containsKey("players_may_spectate")) playersMaySpectate = map.get("players_may_spectate") != Boolean.FALSE;
            if (map.containsKey("server_id")) serverId = ((Number)map.get("server_id")).intValue();
            if (map.containsKey("play_mode")) playMode = (String)map.get("play_mode");
        }

        // Only used in Server serialization
        void store(Map<String, Object> map) {
            map.put("name", name);
            map.put("display_name", displayName);
            map.put("description", description);
            map.put("shorthand", shorthand);
            map.put("priority", priority);
            map.put("setup_script", setupScript);
            map.put("min_players", minPlayers);
            if (connect) map.put("connect", connect);
            if (uniqueId != null) map.put("unique_id", uniqueId.toString());
            map.put("map_id", mapId);
            map.put("debug", debug);
            if (members != null) map.put("members", members.stream().map(Object::toString).collect(Collectors.toList()));
            if (spectators != null) map.put("spectators", spectators.stream().map(Object::toString).collect(Collectors.toList()));
            if (invitees != null) map.put("invitees", invitees.stream().map(Object::toString).collect(Collectors.toList()));
            if (owner != null) map.put("owner", owner.toString());
            if (publicGame) map.put("public", publicGame);
            if (!playersMayJoin) map.put("players_may_join", playersMayJoin); // default is true
            if (!playersMaySpectate) map.put("players_may_spectate", playersMaySpectate); // default is true
            if (serverId >= 0) map.put("server_id", serverId);
            if (playMode != null) map.put("play_mode", playMode);
        }
    }

    @SuppressWarnings("unchecked")
    void loadGames() {
        games.clear();
        for (File file: new File("games").listFiles()) {
            String name = file.getName();
            if (!name.endsWith(".game")) continue;
            name = name.substring(0, name.length() - 5);
            Map<String, Object> map;
            try {
                map = (Map<String, Object>)JSONValue.parseWithException(new FileReader(file));
            } catch (IOException ioe) {
                ioe.printStackTrace();
                continue;
            } catch (ParseException pe) {
                pe.printStackTrace();
                continue;
            } catch (ClassCastException cce) {
                cce.printStackTrace();
                continue;
            }
            Game game = new Game();
            game.name = name;
            game.load(map);
            games.add(game);
        }
        Collections.sort(games, (a, b) -> Integer.compare(a.priority, b.priority));
    }

    @SuppressWarnings("unchecked")
    void loadOpenGames() {
        openGames.clear();
        File file = new File("save/games.save");
        if (!file.isFile()) return;
        List<Map<String, Object> > list;
        try {
            list = (List<Map<String, Object> >)JSONValue.parseWithException(new FileReader(file));
        } catch (IOException ioe) {
            ioe.printStackTrace();
            return;
        } catch (ParseException pe) {
            pe.printStackTrace();
            return;
        }
        if (list == null) return;
        for (Map<String, Object> map: list) {
            Game game = new Game();
            game.load(map);
            openGames.put(game.uniqueId, game);
        }
    }

    void saveOpenGames() {
        List<Map<String, Object> > list = new ArrayList<>();
        for (Game game: openGames.values()) {
            Map<String, Object> map = new HashMap<>();
            game.store(map);
            list.add(map);
        }
        try {
            FileWriter fw = new FileWriter("save/games.save");
            JSONValue.writeJSONString(list, fw);
            fw.flush();
            fw.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    // Worlds

    class WorldInfo {
        String gameName;
        String mapId;
        String mapPath;
        String displayName;
        List<String> authors;
        String description;

        Object worldInfoButton(ChatColor color, boolean withCommand) {
            StringBuilder credits = new StringBuilder();
            if (!authors.isEmpty()) {
                credits.append("Made by:").append(ChatColor.GRAY);
                for (String author: authors) {
                    credits.append(" ").append(author);
                }
                credits.append(ChatColor.RESET);
            }
            String cmd;
            if (withCommand) {
                cmd = "/game map " + mapId;
            } else {
                cmd = null;
            }
            // Format the description to a pleasant tooltip width
            StringBuilder desc = new StringBuilder();
            desc.append(ChatColor.LIGHT_PURPLE);
            desc.append(ChatColor.ITALIC);
            int len = 0;
            for (String word: this.description.split(" ")) {
                if (word.isEmpty()) continue;
                if (len == 0) {
                    len = word.length();
                    desc.append(word);
                } else if (len + 1 + word.length() <= 24) {
                    len += 1 + word.length();
                    desc.append(" ").append(word);
                } else {
                    len = word.length();
                    desc.append("\n").append(word);
                }
            }
            return button(color, displayName, cmd, displayName + "\n" + credits + "\n" + desc);
        }
    }

    @SuppressWarnings("unchecked")
    List<WorldInfo> getWorldInfos() {
        if (worldInfos == null) {
            worldInfos = new ArrayList<>();
            Yaml yaml = new Yaml();
            try {
                Map<String, Object> map = (Map<String, Object>)yaml.load(new FileReader("config/worlds.yml"));
                for (String gameKey: map.keySet()) {
                    Map<String, Object> mapSection = (Map<String, Object>)map.get(gameKey);
                    for (String mapKey: mapSection.keySet()) {
                        Map<String, Object> worldSection = (Map<String, Object>)mapSection.get(mapKey);
                        WorldInfo wi = new WorldInfo();
                        wi.gameName = gameKey;
                        wi.mapId = (String)worldSection.get("MapID");
                        wi.mapPath = (String)worldSection.get("MapPath");
                        wi.authors = (List<String>)worldSection.get("Authors");
                        wi.displayName = (String)worldSection.get("DisplayName");
                        wi.description = (String)worldSection.get("Description");
                        if (wi.mapId == null) continue;
                        if (wi.mapPath == null) continue;
                        if (wi.authors == null) wi.authors = new ArrayList<>();
                        if (wi.displayName == null) wi.displayName = wi.mapId;
                        if (wi.description == null) wi.description = "";
                        worldInfos.add(wi);
                    }
                }
            } catch (Throwable t) {
                t.printStackTrace();
                return worldInfos;
            }
        }
        return worldInfos;
    }

    WorldInfo findWorldInfo(String gameName, String mapId) {
        for (WorldInfo wi: getWorldInfos()) {
            if (wi.gameName.equals(gameName) && wi.mapId.equals(mapId)) return wi;
        }
        return null;
    }

    List<WorldInfo> findGameWorlds(String gameName) {
        List<WorldInfo> result = new ArrayList<>();
        for (WorldInfo wi: getWorldInfos()) {
            if (wi.gameName.equals(gameName)) result.add(wi);
        }
        return result;
    }

    // Play Modes

    final class PlayMode {
        String gameName;
        String modeId;
        String displayName;
        String description;
    }

    @SuppressWarnings("unchecked")
    final List<PlayMode> getPlayModes() {
        if (playModes == null) {
            playModes = new ArrayList<>();
            Yaml yaml = new Yaml();
            Map<String, Object> gamesMap;
            try {
                gamesMap = (Map<String, Object>)yaml.load(new FileReader("config/modes.yml"));
            } catch (IOException ioe) {
                ioe.printStackTrace();
                return playModes;
            }
            for (String gameKey: gamesMap.keySet()) {
                Map<String, Object> modesMap = (Map<String, Object>)gamesMap.get(gameKey);
                for (String modeKey: modesMap.keySet()) {
                    Map<String, Object> modeMap = (Map<String, Object>)modesMap.get(modeKey);
                    PlayMode playMode = new PlayMode();
                    playMode.gameName = gameKey;
                    playMode.modeId = (String)modeMap.get("ModeID");
                    playMode.displayName = (String)modeMap.get("DisplayName");
                    playMode.description = (String)modeMap.get("Description");
                    playModes.add(playMode);
                }
            }
        }
        return playModes;
    }

    final List<PlayMode> findPlayModes(String gameName) {
        return getPlayModes().stream().filter(a -> a.gameName.equals(gameName)).collect(Collectors.toList());
    }

    final PlayMode findPlayMode(String gameName, String modeName) {
        for (PlayMode a: getPlayModes()) {
            if (a.gameName.equals(gameName) && a.modeId.equals(modeName)) return a;
        }
        return null;
    }

    // Synchronous Event Responders

    void syncGameCommand(OnlinePlayer sender, String serverName, String[] args) {
        User user = getUser(sender);
        if (args.length == 0) {
            if (user.currentGame != null) {
                Game game = openGames.get(user.currentGame);
                if (game != null) {
                    sendGameInfo(sender.getUuid(), serverName, game);
                } else {
                    users.remove(sender.getUuid());
                    sendGamesMenu(sender.getUuid(), serverName);
                }
            } else {
                sendGamesMenu(sender.getUuid(), serverName);
            }
            return;
        }
        String cmd = args[0];
        switch (cmd) {
        case "invite":
            if (args.length <= 2) {
                if (user.currentGame == null) {
                    sendMessage(sender.getUuid(), serverName, ChatColor.RED, "Create a game first.");
                    return;
                }
                Game game = openGames.get(user.currentGame);
                if (game == null || !sender.getUuid().equals(game.owner)) {
                    sendMessage(sender.getUuid(), serverName, ChatColor.RED, "You cannot modify this game.");
                    return;
                }
                if (game.serverId >= 0) {
                    sendMessage(sender.getUuid(), serverName, ChatColor.RED, "Game has already started.");
                    return;
                }
                if (args.length == 1) {
                    sendGameInfo(sender.getUuid(), serverName, game, GameInfoMode.INVITE);
                } else if (args.length == 2) {
                    String inviteeName = args[1];
                    OnlinePlayer invitee = null;
                    for (OnlinePlayer op: connect.getOnlinePlayers()) {
                        if (op.getName().equals(inviteeName)) {
                            invitee = op;
                            break;
                        }
                    }
                    if (invitee == null) {
                        sendMessage(sender.getUuid(), serverName, ChatColor.RED, "Player not found: %s", inviteeName);
                        return;
                    }
                    User inviteeUser = getUser(invitee);
                    if (inviteeUser.currentGame != null) {
                        sendMessage(sender.getUuid(), serverName, ChatColor.RED, "%s is already in a game.", invitee.getName());
                        return;
                    }
                    if (game.invitees.contains(invitee.getUuid())) {
                        sendMessage(sender.getUuid(), serverName, ChatColor.RED, "%s is already invited.", invitee.getName());
                        return;
                    }
                    game.invitees.add(invitee.getUuid());
                    dirtyGames = true;
                    Map<String, Object> payload = new HashMap<>();
                    payload.put("target", invitee.getUuid().toString());
                    List<Object> chat = new ArrayList<>();
                    chat.add("");
                    chat.add(sender.getName() + " invited you to a game of " + game.displayName + ". ");
                    chat.add(button(ChatColor.GREEN, "[Accept]", "/game " + game.uniqueId + " join", "Accept the game invite"));
                    chat.add(" ");
                    chat.add(button(ChatColor.YELLOW, "[Info]", "/game " + game.uniqueId + " info", "Read more"));
                    payload.put("chat", chat);
                    connect.broadcast("PLAYER_MESSAGE", payload);
                    sendMessage(sender.getUuid(), serverName, "&aInvited %s to this game.", invitee.getName());
                } else {
                    return;
                }
            }
            break;
        case "public":
            if (args.length == 1) {
                if (user.currentGame == null) {
                    sendMessage(sender.getUuid(), serverName, ChatColor.RED, "Create a game first.");
                    return;
                }
                Game game = openGames.get(user.currentGame);
                if (game == null || !sender.getUuid().equals(game.owner)) {
                    sendMessage(sender.getUuid(), serverName, ChatColor.RED, "You cannot modify this game.");
                    return;
                }
                if (game.publicGame) {
                    sendMessage(sender.getUuid(), serverName, ChatColor.RED, "Game is already public.");
                    return;
                }
                game.publicGame = true;
                dirtyGames = true;
                sendGameInfo(sender.getUuid(), serverName, game);
                if (game.serverId < 0) {
                    for (OnlinePlayer invitee: connect.getOnlinePlayers()) {
                        User inviteeUser = getUser(invitee);
                        if (inviteeUser.currentGame != null) continue;
                        Map<String, Object> payload = new HashMap<>();
                        payload.put("target", invitee.getUuid().toString());
                        List<Object> chat = new ArrayList<>();
                        chat.add("");
                        chat.add(sender.getName() + " opened a game of " + game.displayName + ". ");
                        chat.add(button(ChatColor.GREEN, "[Join]", "/game " + game.uniqueId + " join", "Join this game."));
                        chat.add(" ");
                        chat.add(button(ChatColor.YELLOW, "[Info]", "/game " + game.uniqueId + " info", "Read more"));
                        payload.put("chat", chat);
                        connect.broadcast("PLAYER_MESSAGE", payload);
                    }
                }
            }
            break;
        case "map":
            if (args.length >= 1) {
                if (user.currentGame == null) {
                    sendMessage(sender.getUuid(), serverName, ChatColor.RED, "Create a game first.");
                    return;
                }
                Game game = openGames.get(user.currentGame);
                if (game == null || !sender.getUuid().equals(game.owner)) {
                    sendMessage(sender.getUuid(), serverName, ChatColor.RED, "You cannot modify this game.");
                    return;
                }
                if (game.serverId >= 0) {
                    sendMessage(sender.getUuid(), serverName, ChatColor.RED, "Game has already started.");
                    return;
                }
                if (args.length == 1) {
                    // /game map
                    sendGameInfo(sender.getUuid(), serverName, game, GameInfoMode.MAP);
                } else {
                    StringBuilder sb = new StringBuilder(args[1]);
                    for (int i = 2; i < args.length; i += 1) sb.append(" ").append(args[i]);
                    String mapArg = sb.toString();
                    WorldInfo worldInfo = findWorldInfo(game.name, mapArg);
                    if (worldInfo == null) return;
                    game.mapId = mapArg;
                    dirtyGames = true;
                    sendGameInfo(sender.getUuid(), serverName, game);
                }
            }
            break;
        case "mode":
            if (args.length >= 1) {
                if (user.currentGame == null) return;
                Game game = openGames.get(user.currentGame);
                if (game == null || !sender.getUuid().equals(game.owner)) return;
                if (game.serverId >= 0) return;
                if (args.length == 1) {
                    sendGameInfo(sender.getUuid(), serverName, game, GameInfoMode.PLAY_MODE);
                } else {
                    StringBuilder sb = new StringBuilder(args[1]);
                    for (int i = 2; i < args.length; i += 1) sb.append(" ").append(args[i]);
                    String modeArg = sb.toString();
                    PlayMode playMode = findPlayMode(game.name, modeArg);
                    if (playMode == null) return;
                    game.playMode = modeArg;
                    dirtyGames = true;
                    sendGameInfo(sender.getUuid(), serverName, game);
                }
            }
            break;
        case "quit": case "leave":
            if (args.length == 1) {
                if (user.currentGame == null) return;
                users.remove(sender.getUuid());
                dirtyUsers = true;
                Game game = openGames.get(user.currentGame);
                if (game == null) return;
                game.members.remove(user);
                game.spectators.remove(user);
                dirtyGames = true;
                if (game.serverId < 0) {
                    if (sender.getUuid().equals(game.owner)) {
                        for (UUID member: game.members) {
                            users.remove(member);
                            sendRawMessage(member, null,
                                           button(ChatColor.RED, sender.getName() + " cancelled the game. ", null, null),
                                           button(ChatColor.YELLOW, "[Menu]", "/game", "Back to menu"));
                        }
                        openGames.remove(game.uniqueId);
                        dirtyGames = true;
                    } else {
                        for (UUID member: game.members) {
                            sendMessage(member, null, "%s left the game.", sender.getName());
                        }
                    }
                } else {
                    // The outbound player_leave_game only goes to the
                    // server running the game so they have an
                    // opportunity to kick said player.
                    Map<String, Object> payload = new HashMap<>();
                    payload.put("action", "player_leave_game");
                    payload.put("player", sender.getUuid().toString());
                    payload.put("game", game.uniqueId.toString());
                    connect.send("game" + game.serverId, "minigames", payload);
                }
            }
            break;
        case "start":
            if (args.length == 1) {
                if (user.currentGame == null) {
                    sendMessage(sender.getUuid(), serverName, ChatColor.RED, "Create a game first.");
                    return;
                }
                final Game game = openGames.get(user.currentGame);
                if (game == null) {
                    // Orphaned game? Should never happen.
                    users.remove(sender.getUuid());
                    dirtyUsers = true;
                    sendMessage(sender.getUuid(), serverName, ChatColor.RED, "You are not in a game.");
                    return;
                }
                if (!sender.getUuid().equals(game.owner)) {
                    sendMessage(sender.getUuid(), serverName, ChatColor.RED, "You are not owner of this game.");
                    return;
                }
                if (game.serverId >= 0) {
                    sendMessage(sender.getUuid(), serverName, ChatColor.RED, "This game is already running.");
                    return;
                }
                if (game.members.size() < game.minPlayers) {
                    sendMessage(sender.getUuid(), serverName, ChatColor.RED, "%d players are required to start.", game.minPlayers);
                    return;
                }
                Server server = null;
                for (Server serveri: servers) {
                    if (serveri.currentGame == null) {
                        server = serveri;
                        break;
                    }
                }
                if (server == null) {
                    sendMessage(sender.getUuid(), serverName, "&eServers are busy right now. Please try again later.");
                    return;
                }
                for (UUID member: game.members) {
                    sendMessage(member, null, "Get ready. Your game will start momentarily.");
                }
                startGame(game, server);
            }
            break;
        default:
            // First argument is none of the above.  Therefore, it
            // must be the name of a Minigame, or a UUID.  In the
            // former case, it may be followed by a subcommand, such
            // as create, join, or spectate.
            if (args.length == 1 || args.length == 2) {
                UUID gameId = null;
                Game game = null;
                boolean targetOpenGame = false;
                boolean targetGeneralGame = false;
                try {
                    gameId = UUID.fromString(cmd);
                    game = openGames.get(gameId);
                    targetOpenGame = true;
                } catch (IllegalArgumentException iae) {
                    for (Game gamei: games) {
                        if (gamei.name.equals(cmd) || gamei.shorthand.equals(cmd)) {
                            game = gamei;
                            targetGeneralGame = true;
                            break;
                        }
                    }
                }
                if (game == null) return;
                if (args.length == 1 && targetGeneralGame) {
                    // e.g. /game colorfall
                    sendGameInfo(sender.getUuid(), serverName, game, GameInfoMode.OVERVIEW);
                } else if (args.length == 2) {
                    switch (args[1]) {
                    case "create":
                        if (targetGeneralGame) {
                            if (user.currentGame != null) {
                                sendMessage(sender.getUuid(), serverName, ChatColor.RED, "You are already in a game.");
                                return;
                            }
                            game = createGame(game);
                            game.owner = sender.getUuid();
                            game.members.add(sender.getUuid());
                            user.currentGame = game.uniqueId;
                            dirtyGames = true;
                            dirtyUsers = true;
                            sendGameInfo(sender.getUuid(), serverName, game);
                        }
                        break;
                    case "join": case "j":
                    case "spec": case "spectate":
                        boolean spectate = args[1].startsWith("spec");
                        if (user.currentGame != null) {
                            sendMessage(sender.getUuid(), serverName, ChatColor.RED, "You are already in a game.");
                            return;
                        }
                        if (targetGeneralGame) {
                            String gameName = game.name;
                            game = null;
                            if (spectate) return;
                            for (Game gamei: openGames.values()) {
                                if (gamei.name.equals(gameName)
                                    && (gamei.publicGame || gamei.invitees.contains(sender.getUuid()))
                                    && ((!spectate && gamei.playersMayJoin) || (spectate && gamei.playersMaySpectate))) {
                                    game = gamei;
                                    break;
                                }
                            }
                        }
                        if (game == null) return; // The targetGeneralGame loop may leave game reset to null.
                        if (!spectate && !game.playersMayJoin) return;
                        if (spectate && !game.playersMaySpectate) return;
                        if (!game.publicGame && !game.invitees.contains(sender.getUuid())) return;
                        if (!game.playersMayJoin) {
                            sendGameInfo(sender.getUuid(), serverName, game);
                        }
                        if (game.serverId >= 0) {
                            // Game must be running.  Just send its server the
                            // message.  Risk getting no response if the
                            // server is still booting up.  Player has to
                            // retry a few seconds later.
                            Server server = servers.get(game.serverId);
                            Map<String, Object> payload = new HashMap<>();
                            payload.put("action", spectate ? "player_spectate_game" : "player_join_game");
                            payload.put("player", sender.getUuid().toString());
                            payload.put("game", game.uniqueId.toString());
                            if (server.state == Server.State.BOOT) {
                                if (server.postBootMessages == null) server.postBootMessages = new ArrayList<>();
                                server.postBootMessages.add(payload);
                            } else {
                                connect.send("game" + game.serverId, "minigames", payload);
                            }
                        } else {
                            user.currentGame = game.uniqueId;
                            game.members.add(sender.getUuid());
                            if (spectate) game.spectators.add(sender.getUuid());
                            sendGameInfo(sender.getUuid(), serverName, game);
                            dirtyUsers = true;
                            dirtyGames = true;
                            for (UUID member: game.members) {
                                if (member.equals(sender.getUuid())) continue;
                                if (spectate) {
                                    sendMessage(member, null, "%s will spectate your game.", sender.getName());
                                } else {
                                    sendMessage(member, null, "%s has joined your game.", sender.getName());
                                }
                            }
                        }
                        break;
                    case "info":
                        if (targetOpenGame) {
                            if (!game.publicGame && !sender.getUuid().equals(game.owner) && !game.members.contains(sender.getUuid()) && !game.invitees.contains(sender.getUuid())) {
                                return;
                            }
                            sendGameInfo(sender.getUuid(), serverName, game);
                        }
                        break;
                    default:
                        break;
                    }
                }
            }
        }
    }

    void sendGamesMenu(UUID target, String serverName) {
        sendMessage(target, serverName, "");
        sendMessage(target, serverName, "&9> &a&lGames Menu");
        List<Game> availableGames = new ArrayList<>();
        for (Game game: openGames.values()) {
            if (game.publicGame || game.members.contains(target) || game.invitees.contains(target)) availableGames.add(game);
        }
        // Public game list
        Collections.shuffle(niceColors, random);
        int i = 0;
        if (!availableGames.isEmpty()) {
            List<Object> gamesJs = new ArrayList<>();
            gamesJs.add("");
            gamesJs.add(format("&9> &fOpen Games&7"));
            for (Game game: availableGames) {
                i += 1;
                gamesJs.add(" ");
                StringBuilder tool = new StringBuilder(game.displayName);
                for (UUID member: game.members) tool.append("\n").append(getUserName(member));
                gamesJs.add(button(niceColors.get(i % niceColors.size()),
                                   "[" + game.displayName + "]",
                                   "/game " + game.uniqueId + " info",
                                   tool.toString()));
            }
            sendRawMessage(target, serverName, gamesJs);
        }
        sendMessage(target, serverName, "&9>");
        sendMessage(target, serverName, "&9> &fAvailable Games &7&o(Click to View)&f:");
        Collections.shuffle(niceColors, random);
        i = 0;
        for (Game game: games) {
            i += 1;
            sendRawMessage(target,
                           serverName,
                           Arrays.asList(
                                         "", format("&9> &f%d) ", i),
                                         button(niceColors.get(i % niceColors.size()),
                                                "[" + game.shorthand + "]&o " + game.displayName,
                                                "/game " + game.name,
                                                game.displayName + "\n&7" + game.description)));
        }
        sendMessage(target, serverName, "");
    }

    enum GameInfoMode {
        NONE, MAP, INVITE, OVERVIEW, PLAY_MODE;
    }

    void sendGameInfo(UUID target, String serverName, Game game) {
        sendGameInfo(target, serverName, game, GameInfoMode.NONE);
    }

    void sendGameInfo(UUID target, String serverName, Game game, GameInfoMode select) {
        sendMessage(target, serverName, "");
        // Figure out if this the setup screen or just general game overview.
        boolean isSetup = select != GameInfoMode.OVERVIEW;
        if (isSetup) {
            sendMessage(target, serverName, ChatColor.GREEN, "&9> &a&l%s Game Setup", game.displayName);
        } else {
            sendMessage(target, serverName, ChatColor.GREEN, "&9> &a&l%s Game Info", game.displayName);
        }
        // Description
        StringBuilder desc = new StringBuilder();
        int descLen = 0;
        for (String word: game.description.split(" ")) {
            if (descLen == 0) {
                desc.append(word);
                descLen = word.length();
            } else if (descLen + 1 + word.length() < 32) {
                desc.append(" ").append(word);
                descLen += 1 + word.length();
            } else {
                desc.append("\n").append(word);
                descLen = word.length();
            }
        }
        for (String line: desc.toString().split("\n")) {
            sendMessage(target, serverName, ChatColor.GRAY, "&9> &7%s", line);
        }
        // Permissions
        boolean canModify = isSetup && target.equals(game.owner);
        boolean isMember = isSetup && game.members.contains(target);
        boolean isInvited = isSetup && (game.publicGame || game.invitees.contains(target));
        if (isSetup) {
            // Player List
            sendMessage(target, serverName, ChatColor.BLUE, ">");
            List<Object> playersJs = new ArrayList<>();
            playersJs.add(format("&9> &fPlayers  "));
            boolean comma = false;
            int i = 0;
            Collections.shuffle(niceColors, random);
            for (UUID uuid: game.members) {
                i += 1;
                if (comma) {
                    playersJs.add(format("&7, "));
                } else {
                    comma = true;
                }
                String userName = getUserName(uuid);
                if (game.spectators.contains(uuid)) {
                    playersJs.add(button(ChatColor.DARK_GRAY, "&o" + getUserName(uuid), null, userName + " has joined this game."));
                } else {
                    playersJs.add(button(niceColors.get(i % niceColors.size()), getUserName(uuid), null, userName + " has joined this game."));
                }
            }
            if (!game.publicGame && canModify && game.playersMayJoin) {
                playersJs.add("  ");
                playersJs.add(button(ChatColor.GREEN, "[Invite]", "/game invite", "Invite a player to your game."));
                playersJs.add("  ");
                playersJs.add(button(ChatColor.BLUE, "[Public]", "/game public", "Invite everyone and allow them to join."));
            }
            if (game.publicGame) {
                playersJs.add("  ");
                playersJs.add(button(ChatColor.GRAY, "&o(Public Game)", null, "Anyone may join this game"));
            }
            sendRawMessage(target, serverName, playersJs);
        }
        if (select == GameInfoMode.INVITE) {
            List<OnlinePlayer> invitees = new ArrayList<>();
            for (OnlinePlayer op: connect.getOnlinePlayers()) {
                User user = getUser(op);
                if (user.currentGame == null) invitees.add(op);
            }
            List<Object> inviteJs = new ArrayList<>();
            inviteJs.add("");
            inviteJs.add(format("&9>"));
            for (OnlinePlayer invitee: invitees) {
                inviteJs.add(" ");
                inviteJs.add(button(ChatColor.GREEN, invitee.getName(),
                                    "/game invite " + invitee.getName(),
                                    "Invite " + invitee.getName()));
            }
            sendRawMessage(target, serverName, inviteJs);
        }
        if (isSetup) {
            // Current Map info
            if (select != GameInfoMode.MAP) {
                Object currentMapButton = button(ChatColor.GRAY, "Random", null, format("Random\n&d&oA random map will be\npicked from the map pool."));
                WorldInfo currentWorldInfo = null;
                if (game.mapId != null) {
                    currentWorldInfo = findWorldInfo(game.name, game.mapId);
                    if (currentWorldInfo == null) {
                        game.mapId = null; // Saving?
                    } else {
                        currentMapButton = currentWorldInfo.worldInfoButton(ChatColor.GRAY, false);
                    }
                }
                if (canModify) {
                    sendRawMessage(target, serverName,
                                   Arrays.asList("",
                                                 format("&9> &fMap "),
                                                 currentMapButton,
                                                 " ",
                                                 button(ChatColor.AQUA, "[Select]", "/game map", "Select a map.")));
                } else {
                    sendRawMessage(target, serverName,
                                   Arrays.asList("",
                                                 format("&9> &fMap "),
                                                 currentMapButton));
                }
                // Map selection
            } else {
                List<Object> mapsJs = new ArrayList<>();
                mapsJs.add("");
                mapsJs.add(format("&9> &fSelect a map:"));
                ChatColor[] colors = { ChatColor.BLUE, ChatColor.GREEN, ChatColor.GOLD, ChatColor.AQUA };
                int i = 0;
                for (WorldInfo worldInfo: findGameWorlds(game.name)) {
                    i += 1;
                    mapsJs.add("  ");
                    mapsJs.add(worldInfo.worldInfoButton(colors[i % colors.length], true));
                }
                sendRawMessage(target, serverName, mapsJs);
            }
            // Play Modes
            if (select == GameInfoMode.PLAY_MODE) {
                List<Object> modeJs = new ArrayList<>();
                modeJs.add("");
                modeJs.add(button(ChatColor.BLUE, "> &fSelect gameplay mode", null, null));
                int i = 0;
                for (PlayMode playMode: findPlayModes(game.name)) {
                    i += 1;
                    modeJs.add("  ");
                    modeJs.add(button(niceColors.get(i % niceColors.size()),
                                      "[" + playMode.displayName + "]",
                                      "/game mode " + playMode.modeId,
                                      playMode.displayName + "\n" + ChatColor.GRAY + playMode.description));
                }
                sendRawMessage(target, serverName, modeJs);
            } else {
                if (!findPlayModes(game.name).isEmpty());
                List<Object> modeJs = new ArrayList<>();
                modeJs.add("");
                modeJs.add(button(ChatColor.BLUE, "> &fMode ", null, null));
                if (game.playMode == null) {
                    modeJs.add(button(ChatColor.GRAY, "Random", null, "A random mode will be picked"));
                } else {
                    PlayMode playMode = findPlayMode(game.name, game.playMode);
                    modeJs.add(button(ChatColor.GRAY, playMode.displayName, null, playMode.displayName + "\n" + ChatColor.GRAY + playMode.description));
                }
                if (canModify) {
                    modeJs.add("  ");
                    modeJs.add(button(ChatColor.GOLD, "[Switch]", "/game mode", "Select a play mode"));
                }
                sendRawMessage(target, serverName, modeJs);
            }
        }
        sendMessage(target, serverName, ChatColor.BLUE, ">");
        if (isSetup) {
            // Action buttons. Go, Cancel, Join, Quit, Spectate
            if (canModify) {
                // Owner stuff
                if (game.serverId < 0) {
                    sendRawMessage(target, serverName, Arrays.asList("",
                                                                     format("&9> &fReady?  "),
                                                                     button(ChatColor.GREEN, "[Go!]", "/game start", "Start the game"),
                                                                     "  ",
                                                                     button(ChatColor.RED, "[Cancel]", "/game quit", "Cancel this game"),
                                                                     "  ",
                                                                     button(ChatColor.YELLOW, "[Refresh]", "/game", "Refresh game info")));
                } else {
                    sendRawMessage(target, serverName, Arrays.asList("",
                                                                     format("&9> &fGame running. "),
                                                                     button(ChatColor.RED, "[Quit]", "/game quit", "Quit this game"),
                                                                     "  ",
                                                                     button(ChatColor.YELLOW, "[Refresh]", "/game", "Refresh game info")));

                }
            } else if (isMember) {
                // Member stuff
                sendRawMessage(target, serverName, Arrays.asList("",
                                                                 format("&9> &fChanged your mind?  "),
                                                                 button(ChatColor.RED, "[Quit]", "/game quit", "Leave this game"),
                                                                 "  ",
                                                                 button(ChatColor.YELLOW, "[Refresh]", "/game", "Refresh game info")));

            } else {
                // Outsider stuff
                List<Object> joinJs = new ArrayList<>();
                joinJs.add("");
                if (isInvited && (game.playersMayJoin || game.playersMaySpectate)) {
                    joinJs.add(format("&9> &fJoin us?  "));
                } else {
                    joinJs.add(format("&9> &fThis game is closed"));
                }
                if (isInvited && game.playersMayJoin) {
                    joinJs.add(button(ChatColor.GREEN, "[Join]", "/game " + game.uniqueId + " join", "Join this game"));
                }
                if (isInvited && game.playersMaySpectate) {
                    joinJs.add("  ");
                    joinJs.add(button(ChatColor.AQUA, "[Spec]", "/game " + game.uniqueId + " spec", "Spectate this game"));
                }
                joinJs.add("  ");
                joinJs.add(button(ChatColor.YELLOW, "[Refresh]", "/game", "Refresh game info"));
                sendRawMessage(target, serverName, joinJs);
            }
        } else {
            sendRawMessage(target, serverName, Arrays.asList("",
                                                             format("&9> &fWanna play?  "),
                                                             button(ChatColor.GREEN, "[Create]", "/game " + game.name + " create", "Create a game")));
            // Create game
        }
        sendMessage(target, serverName, "");
    }

    // Messaging

    void sendRawMessage(UUID target, String server, Object... obj) {
        Map<String, Object> js = new HashMap<>();
        js.put("target", target.toString());
        Object message;
        if (obj.length == 0) {
            return;
        } else if (obj.length == 1) {
            message = obj[0];
        } else {
            message = Arrays.asList(obj);
        }
        js.put("chat", message);
        if (server == null) {
            connect.broadcast("PLAYER_MESSAGE", js);
        } else {
            connect.send(server, "PLAYER_MESSAGE", js);
        }
    }

    void sendMessage(UUID target, String server, String message, Object... args) {
        sendMessage(target, server, ChatColor.WHITE, message, args);
    }

    void sendMessage(UUID target, String server, ChatColor color, String message, Object... args) {
        message = ChatColor.translateAlternateColorCodes('&', message);
        if (args.length > 0) message = String.format(message, args);
        Map<String, Object> js = new HashMap<>();
        js.put("text", message);
        js.put("color", color.name().toLowerCase());
        sendRawMessage(target, server, js);
    }

    static String format(String msg, Object... args) {
        msg = ChatColor.translateAlternateColorCodes('&', msg);
        if (args.length > 0) msg = String.format(msg, args);
        return msg;
    }

    static Object button(ChatColor color, String chat, String command, String tooltip) {
        Map<String, Object> map = new HashMap<>();
        if (color != null) {
            map.put("color", color.name().toLowerCase());
        }
        map.put("text", ChatColor.translateAlternateColorCodes('&', chat));
        if (command != null) {
            Map<String, Object> clickEvent = new HashMap<>();
            map.put("clickEvent", clickEvent);
            clickEvent.put("action", "run_command");
            clickEvent.put("value", command);
        }
        if (tooltip != null) {
            Map<String, Object> hoverEvent = new HashMap<>();
            map.put("hoverEvent", hoverEvent);
            hoverEvent.put("action", "show_text");
            hoverEvent.put("value", ChatColor.translateAlternateColorCodes('&', tooltip));
        }
        return map;
    }

    // Game and Server Startup

    // A modified copy of the input argument will be created here.
    Game createGame(Game template) {
        Game game = new Game(template);
        game.uniqueId = new UUID(System.nanoTime(), random.nextLong());
        game.members = new ArrayList<>();
        game.spectators = new ArrayList<>();
        game.invitees = new ArrayList<>();
        game.mapId = null;
        game.serverId = -1;
        openGames.put(game.uniqueId, game);
        return game;
    }

    void startGame(final Game game, final Server server) {
        server.currentGame = game.uniqueId;
        game.serverId = server.index;
        server.state = Server.State.BOOT;
        dirtyServers = true;
        dirtyGames = true;
        // Running e.g.: ./script/base-game.setup colorfall /home/creative/minecraft/worlds/Colorhunt
        WorldInfo worldInfo;
        if (game.mapId != null) {
            worldInfo = findWorldInfo(game.name, game.mapId);
        } else {
            List<WorldInfo> infos = findGameWorlds(game.name);
            worldInfo = infos.get(random.nextInt(infos.size()));
            game.mapId = worldInfo.mapId;
        }
        List<PlayMode> gamePlayModes = findPlayModes(game.name);
        if (!gamePlayModes.isEmpty() && game.playMode == null) {
            game.playMode = gamePlayModes.get(random.nextInt(gamePlayModes.size())).modeId;
        }
        final ProcessBuilder pb = new ProcessBuilder("script/" + game.setupScript, game.name, "" + server.index, worldInfo.mapPath);
        pb.inheritIO();
        final int serverIndex = server.index;
        Runnable run = () -> {
            int ret;
            try {
                Process process = pb.start();
                ret = process.waitFor();
            } catch (InterruptedException ie) {
                ie.printStackTrace();
                ret = -1;
            } catch (IOException ioe) {
                ioe.printStackTrace();
                ret = -1;
            }
            // Store configurations
            if (ret == 0) {
                Map<String, Object> gameConfigMap = new HashMap<>();
                game.store(gameConfigMap);
                try {
                    FileWriter fw;
                    fw = new FileWriter("run/game" + server.index + "/game_config.json");
                    JSONValue.writeJSONString(gameConfigMap, fw);
                    fw.flush();
                    fw.close();
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                }
                try {
                    final ProcessBuilder pb2 = new ProcessBuilder("script/game.start", "" + server.index, "" + (2600 + server.index));
                    pb2.inheritIO();
                    Process process = pb2.start();
                    ret = process.waitFor();
                } catch (InterruptedException ie) {
                    ie.printStackTrace();
                    ret = -1;
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                    ret = -1;
                }
            }
            final int retval = ret; // Value type must be final :(
            tasks.add(() -> syncGameSetupFinished(retval, server.index, game.uniqueId));
        };
        new Thread(run).start();
    }

    void syncGameSetupFinished(int retval, int serverIndex, UUID gameId) {
        final Server server = servers.get(serverIndex);
        final Game game = openGames.get(gameId);
        if (retval != 0) {
            for (UUID member: game.members) {
                sendMessage(member, null, ChatColor.RED, "Your game could not be created. Please contact an administrator, or try again later.");
                users.remove(member);
            }
            server.reset();
        } else {
            server.state = Server.State.RUN;
            for (UUID member: game.members) {
                sendMessage(member, null, "Your game is ready.");
                sendPlayerToServer(member, "game" + server.index);
            }
        }
        dirtyServers = true;
    }

    void sendPlayerToServer(UUID playerId, String serverName) {
        Map<String, String> map = new HashMap<>();
        map.put("player", playerId.toString());
        map.put("server", serverName);
        connect.broadcast("SEND_PLAYER_SERVER", map);
    }

    // Messages sent by a running game (server) to the daemon.
    @SuppressWarnings("unchecked")
    void syncMinigamesMessage(Message message) {
        Map<String, Object> map = (Map<String, Object>)message.getPayload();
        String action = (String)map.get("action");
        switch (action) {
            // The inbound player_leave_game comes from the server
            // running the game.  They will have done most of the work
            // already, which is to kick the player.
        case "player_leave_game":
            if (map.containsKey("player") && map.containsKey("game")) {
                UUID player = UUID.fromString((String)map.get("player"));
                UUID gameId = UUID.fromString((String)map.get("game"));
                User user = users.get(player);
                if (user == null || user.currentGame == null || !user.currentGame.equals(gameId)) return;
                users.remove(player);
                dirtyUsers = true;
                Game game = openGames.get(gameId);
                if (game == null) return;
                game.members.remove(player);
                game.spectators.remove(player);
            }
            break;
        case "game_add_player":
            // We assume that the player issued the command to join an
            // already game.  The plugin on the server running said
            // game did the necessary internal checks and then sent
            // this message to the Daemon.
            if (map.containsKey("player") && map.containsKey("game")) {
                UUID playerId = UUID.fromString((String)map.get("player"));
                UUID gameId = UUID.fromString((String)map.get("game"));
                User user = getUser(playerId);
                if (user.currentGame != null) {
                    sendMessage(playerId, null, ChatColor.RED, "You are already in a game.");
                    return;
                }
                Game game = openGames.get(gameId);
                if (game == null) {
                    sendMessage(playerId, null, ChatColor.RED, "Game not found.");
                    return;
                }
                user.currentGame = gameId;
                game.members.add(playerId);
                dirtyUsers = true;
                dirtyGames = true;
                if (game.serverId >= 0) { // Should always be true in this situation.
                    sendPlayerToServer(playerId, "game" + game.serverId);
                }
            }
            break;
        case "game_add_spectator":
            if (map.containsKey("player") && map.containsKey("game")) {
                UUID playerId = UUID.fromString((String)map.get("player"));
                UUID gameId = UUID.fromString((String)map.get("game"));
                User user = getUser(playerId);
                if (user.currentGame != null) {
                    sendMessage(playerId, null, ChatColor.RED, "You are already in a game.");
                    return;
                }
                Game game = openGames.get(gameId);
                if (game == null) {
                    sendMessage(playerId, null, ChatColor.RED, "Game not found.");
                    return;
                }
                user.currentGame = gameId;
                game.members.add(playerId);
                game.spectators.add(playerId);
                dirtyUsers = true;
                dirtyGames = true;
                if (game.serverId >= 0) { // Should always be true in this situation.
                    sendPlayerToServer(playerId, "game" + game.serverId);
                }
            }
            break;
        case "game_end":
            if (map.containsKey("game")) {
                UUID gameId = UUID.fromString((String)map.get("game"));
                if (gameId == null) return;
                Game game = openGames.get(gameId);
                if (game == null) return;
                for (UUID memberId: game.members) {
                    users.remove(memberId);
                    dirtyUsers = true;
                }
                openGames.remove(gameId);
                dirtyGames = true;
            }
            break;
        case "game_config":
            if (map.containsKey("game") && map.containsKey("key") && map.containsKey("value")) {
                UUID gameId = UUID.fromString((String)map.get("game"));
                if (gameId == null) return;
                Game game = openGames.get(gameId);
                if (game == null) return;
                Map<String, Object> config = new HashMap<>();
                config.put((String)map.get("key"), map.get("value"));
                game.load(config);
                dirtyGames = true;
            }
            break;
        default:
            break;
        }
    }

    void syncConsoleCommand(String cmd, String[] args) {
        switch (cmd) {
        case "stop":
            if (args.length == 0) {
                System.out.println("Stopping Daemon...");
                shouldStop = true;
                return;
            }
            break;
        case "list":
            if (args.length == 0) {
                System.out.println("Server Player List");
                for (ServerConnection server: connect.getServer().getConnections()) {
                    System.out.println("= " + server.getName() + " (" + server.getOnlinePlayers().size() + ") " + server.getStatus());
                    for (OnlinePlayer player: server.getOnlinePlayers()) {
                        System.out.println("  - " + player.getName());
                    }
                }
                return;
            }
            break;
        case "info":
            if (args.length == 0) {
                System.out.println("Users " + users.size());
                for (User user: users.values()) {
                    Map<String, Object> map = new HashMap<>();
                    user.store(map);
                    System.out.println("  " + JSONValue.toJSONString(map));
                }
                System.out.println("Servers " + servers.size());
                for (Server server: servers) {
                    Map<String, Object> map = new HashMap<>();
                    server.store(map);
                    System.out.println("  " + JSONValue.toJSONString(map));
                }
                System.out.println("Games " + openGames.size());
                for (Game game: openGames.values()) {
                    Map<String, Object> map = new HashMap<>();
                    game.store(map);
                    System.out.println("  " + JSONValue.toJSONString(map));
                }
                return;
            }
            break;
        case "save":
            if (args.length == 0) {
                System.out.println("Saving servers...");
                dirtyServers = true;
                System.out.println("Saving users...");
                dirtyUsers = true;
                return;
            }
            break;
        case "reload":
            if (args.length == 0) {
                System.out.println("Loading servers...");
                loadServers();
                System.out.println("Loading users...");
                loadUsers();
                System.out.println("Loading games...");
                loadGames();
                System.out.println("Loading open games...");
                loadOpenGames();
                System.out.println("Flushing all cached config files...");
                playerCache = null;
                worldInfos = null;
                playModes = null;
                return;
            }
            break;
        case "sleep":
            if (args.length == 0) {
                System.out.println("Sleeping for 10 seconds...");
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException ie) {
                    ie.printStackTrace();
                }
                return;
            }
            break;
        case "reset":
            if (args.length == 0) {
                System.out.println("Resetting all runtime data...");
                users.clear();
                for (Server server: servers) {
                    server.reset();
                }
                openGames.clear();
                dirtyUsers = true;
                dirtyServers = true;
                dirtyGames = true;
                playerCache = null;
                worldInfos = null;
                playModes = null;
                return;
            }
            break;
        case "exec":
            if (args.length > 0) {
                System.out.println("Executing " + Arrays.asList(args));
                ProcessBuilder pb = new ProcessBuilder(args);
                pb.redirectOutput(ProcessBuilder.Redirect.PIPE);
                pb.redirectError(ProcessBuilder.Redirect.PIPE);
                try {
                    pb.start();
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                }
                return;
            }
            break;
        case "worlds":
            if (args.length == 0) {
                int i = 0;
                System.out.println("Worlds:");
                for (WorldInfo wi: getWorldInfos()) {
                    i += 1;
                    System.out.println("#" + i + " game=" + wi.gameName + " id=" + wi.mapId + " path=" + wi.mapPath);
                }
            }
        default:
            break;
        }
        usage();
    }

    boolean gameSessionExists(int id) {
        try {
            ProcessBuilder pb = new ProcessBuilder("script/game.exists", "" + id);
            int retval = pb.start().waitFor();
            return retval == 0;
        } catch (IOException ioe) {
            ioe.printStackTrace();
            return false;
        } catch (InterruptedException ie) {
            ie.printStackTrace();
            return false;
        }
    }

    void stopGameSession(int id) {
        try {
            ProcessBuilder pb = new ProcessBuilder("script/game.stop", "" + id);
            pb.start();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    void usage() {
        System.out.println("Available Commands:");
        System.out.println("stop - stop the server");
        System.out.println("list - list servers and players");
        System.out.println("info - dump info about users, servers, games");
        System.out.println("save - save servers and users");
        System.out.println("reload - reload all configs");
        System.out.println("sleep - sleep for 10 seconds");
        System.out.println("exec - execute file");
    }

    // Database

    @SuppressWarnings("unchecked")
    Connection getDatabaseConnection(String database) throws Exception {
        Class.forName("com.mysql.jdbc.Driver");
        Map<String, Object> map;
        map = (Map<String, Object>)JSONValue.parseWithException(new FileReader("config/sql.conf"));
        return DriverManager.getConnection("jdbc:mysql://" + map.get("host") + ":" + map.get("port") + "/" + database, (String)map.get("user"), (String)map.get("password"));
    }

    Map<UUID, String> getPlayerCache() {
        if (playerCache == null) {
            playerCache = new HashMap<>();
            try {
                ResultSet result = getDatabaseConnection("PlayerCache").createStatement().executeQuery("SELECT `uuid`, `name` FROM `players`");
                while (result.next()) {
                    UUID uuid = UUID.fromString(result.getString("uuid"));
                    String name = result.getString("name");
                    playerCache.put(uuid, name);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return playerCache;
    }

    String getUserName(UUID uuid) {
        User user = users.get(uuid);
        if (user != null && user.name != null) return user.name;
        String result = getPlayerCache().get(uuid);
        if (result != null) return result;
        return "N/A";
    }
}
