

import java.nio.file.*;

public class Verify {
    public static void main(String[] args) throws Exception {
        if (args.length == 0 || (args.length % 2) != 0) {
            System.err.println("usage: Verify <name> <path> [<name> <path> ...]");
            System.exit(2);
        }
        int fails = 0;
        Loader loader = new Loader();
        for (int i = 0; i < args.length; i += 2) {
            String name = args[i];
            String path = args[i + 1];
            try {
                byte[] bytes = Files.readAllBytes(Paths.get(path));
                Class<?> c = loader.defineFromBytes(name, bytes);

                Class.forName(c.getName(), true, loader);
                c.getDeclaredMethods();
                if (name.equals("RelayHandler")) {
                    c.getDeclaredMethod("channelInactive", io.netty.channel.ChannelHandlerContext.class);
                    c.getDeclaredMethod("channelRead", io.netty.channel.ChannelHandlerContext.class, Object.class);
                    c.getDeclaredMethod("write", io.netty.channel.ChannelHandlerContext.class, Object.class, io.netty.channel.ChannelPromise.class);
                    c.getDeclaredConstructor().newInstance();
                }
                System.out.println("OK   " + name + "  (loaded " + c.getName() + ")");
            } catch (Throwable t) {
                System.out.println("FAIL " + name + "  " + t.getClass().getName()
                    + ": " + t.getMessage());
                fails++;
            }
        }
        if (fails != 0) {
            System.out.println(fails + " verification failure(s)");
            System.exit(1);
        }
    }

    static class Loader extends ClassLoader {
        Loader() { super(ClassLoader.getSystemClassLoader()); }
        Class<?> defineFromBytes(String hint, byte[] b) {
            return defineClass(null, b, 0, b.length);
        }
    }
}
