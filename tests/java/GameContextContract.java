// Stand-in for net.minecraft.network.protocol.game.GameProtocols$Context.
// emit_samples emits gen/GameContext implementing this, so the class-file
// writer's interface support (ClassBuilder::addInterface) is exercised the same
// way native/b_server.cpp exercises it at runtime -- without needing a
// Minecraft jar in the test harness.
public interface GameContextContract {
    boolean hasInfiniteMaterials();
}
