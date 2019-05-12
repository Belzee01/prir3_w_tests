import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;

public class PMO_Barrier implements PMO_LogSource {
	public final CyclicBarrier barrier;
	public final boolean withTrigger;
	public final AtomicBoolean enabled;
	public final String name;

	/**
	 * Bariera z dodatkami
	 * 
	 * @param parties     liczba synchronizowanych watkow
	 * @param enabled     czy jest aktywna
	 * @param singleUsage czy przeznaczona do pojedynczego uzycia
	 * @param withTrigger czy ma dzialac automatycznie czy czekac na wyzwolenie
	 */
	public PMO_Barrier(int parties, boolean enabled, boolean singleUsage, boolean withTrigger, String name) {
		this.enabled = new AtomicBoolean(enabled);
		this.withTrigger = withTrigger;
		this.name = name;

		Runnable toDo;
		if (singleUsage) {
			toDo = () -> {
				this.enabled.set(false);
			};
		} else {
			toDo = () -> {
			};
		}
		if (withTrigger) {
			barrier = new CyclicBarrier(parties + 1, toDo);
			log("Utworzono bariere " + name + " dla " + (parties + 1) + " watków");
		} else {
			barrier = new CyclicBarrier(parties, toDo);
			log("Utworzono bariere " + name + " dla " + parties + " watków");
		}
	}

	public void enable(boolean singleUsage) {
		log("Enable dla bariery " + name);
		enabled.set(true);
	}

	public void disable() {
		enabled.set(false);
	}

	public void await() {
		if (enabled.get()) {
			log("Przed await() dla bariery " + name + " getParties = " + barrier.getParties() + " getNumberWaiting = "
					+ barrier.getNumberWaiting());
			PMO_BarrierHelper.await(barrier);
			log("Po await() dla bariery " + name);
		}
	}

	public void trigger() {
		if (withTrigger) {
			log("Aktywowano trigger dla bariery " + name);
			log("Trigger przed while dla bariery " + name);
			log("Trigger dla bariery " + name + " getParties = " + barrier.getParties() + " getNumberWaiting = "
					+ barrier.getNumberWaiting());
			while (barrier.getParties() - barrier.getNumberWaiting() != 1) {
				PMO_TimeHelper.sleep(150);
				log("Trigger dla bariery " + name + " getParties = " + barrier.getParties() + " getNumberWaiting = "
						+ barrier.getNumberWaiting());
			}
			log("Trigger dla bariery " + name + " getParties = " + barrier.getParties() + " getNumberWaiting = "
					+ barrier.getNumberWaiting());
			log("Trigger przed await dla bariery " + name);
			PMO_BarrierHelper.await(barrier);
			log("Trigger po await dla bariery " + name);
		}
	}

	public void trigger(boolean asynchronous) {
		if (asynchronous) {
			Thread th = new Thread(this::trigger);
			th.setDaemon(true);
			th.start();
		} else {
			trigger();
		}
	}

}
