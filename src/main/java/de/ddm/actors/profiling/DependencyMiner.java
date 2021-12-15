package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.Terminated;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.InputConfigurationSingleton;
import de.ddm.singletons.SystemConfigurationSingleton;
import de.ddm.structures.InclusionDependency;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.EqualsAndHashCode;

import java.io.File;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;

public class DependencyMiner extends AbstractBehavior<DependencyMiner.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable, LargeMessageProxy.LargeMessage {
	}

	@NoArgsConstructor
	public static class StartMessage implements Message {
		private static final long serialVersionUID = -1963913294517850454L;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class HeaderMessage implements Message {
		private static final long serialVersionUID = -5322425954432915838L;
		int id;
		String[] header;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class BatchMessage implements Message {
		private static final long serialVersionUID = 4591192372652568030L;
		int id;
		List<String[]> batch;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class RegistrationMessage implements Message {
		private static final long serialVersionUID = -4025238529984914107L;
		ActorRef<DependencyWorker.Message> dependencyWorker;
		ActorRef<LargeMessageProxy.Message> dependencyWorkerLargeMessageProxy;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class CompletionMessage implements Message {
		private static final long serialVersionUID = -7642425159675583598L;
		ActorRef<DependencyWorker.Message> dependencyWorker;

		List<ddp.algo.UnaryInclusion.Dependency> aInB;
		List<ddp.algo.UnaryInclusion.Dependency> bInA;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "dependencyMiner";

	public static final ServiceKey<DependencyMiner.Message> dependencyMinerService = ServiceKey.create(DependencyMiner.Message.class, DEFAULT_NAME + "Service");

	public static Behavior<Message> create() {
		return Behaviors.setup(DependencyMiner::new);
	}

	private DependencyMiner(ActorContext<Message> context) {
		super(context);
		this.discoverNaryDependencies = SystemConfigurationSingleton.get().isHardMode();
		this.inputFiles = InputConfigurationSingleton.get().getInputFiles();
		this.headerLines = new String[this.inputFiles.length][];
		this.contentLines = new ArrayList<>();
		for (int id = 0; id < this.inputFiles.length; id++)
			this.contentLines.add(new ArrayList<>());
        this.finishedReading = new boolean[this.inputFiles.length];

		this.inputReaders = new ArrayList<>(inputFiles.length);
		for (int id = 0; id < this.inputFiles.length; id++)
			this.inputReaders.add(context.spawn(InputReader.create(id, this.inputFiles[id]), InputReader.DEFAULT_NAME + "_" + id));
		this.resultCollector = context.spawn(ResultCollector.create(), ResultCollector.DEFAULT_NAME);
		this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);

		this.dependencyWorkers = new ArrayList<>();
		this.dependencyWorkerLargeProxies = new ArrayList<>();
		this.unassignedTasks = new ArrayDeque<>();
		this.busyWorkers = new HashMap<>();

		context.getSystem().receptionist().tell(Receptionist.register(dependencyMinerService, context.getSelf()));
	}

	/////////////////
	// Actor State //
	/////////////////

	@AllArgsConstructor
	@EqualsAndHashCode
	public static class Task {
		int tableA;
		int tableB;
	}

	private long startTime;

	private final boolean discoverNaryDependencies;
	private final File[] inputFiles;      // TODO should be List
	private final String[][] headerLines; // TODO should be List
	private final List<List<String[]>> contentLines;
    private final boolean[] finishedReading;

	private final List<ActorRef<InputReader.Message>> inputReaders;
	private final ActorRef<ResultCollector.Message> resultCollector;
	private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;

	// list of all registered DependencyWorkers
	private final List<ActorRef<DependencyWorker.Message>> dependencyWorkers;
	private final List<ActorRef<LargeMessageProxy.Message>> dependencyWorkerLargeProxies;
	// all Tasks that not yet assigned
	private final Queue<Task> unassignedTasks;
	// all Workers that are busy, with their assigned Task
	private final Map<ActorRef<DependencyWorker.Message>, Task> busyWorkers;

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(StartMessage.class, this::handle)
				.onMessage(HeaderMessage.class, this::handle)
				.onMessage(BatchMessage.class, this::handle)
				.onMessage(RegistrationMessage.class, this::handle)
				.onMessage(CompletionMessage.class, this::handle)
				.onSignal(Terminated.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(StartMessage message) {
		for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
			inputReader.tell(new InputReader.ReadHeaderMessage(this.getContext().getSelf()));
		for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
			inputReader.tell(new InputReader.ReadBatchMessage(this.getContext().getSelf()));
		this.startTime = System.currentTimeMillis();
		return this;
	}

	private Behavior<Message> handle(HeaderMessage message) {
		this.getContext().getLog().info("Read header of size {} from table {}", message.header.length, message.id);

		this.headerLines[message.getId()] = message.getHeader();
		return this;
	}

	// try to delegate all unassigned Tasks to idle DependencyWorkers
	private void delegateTasks(){
		for (int workerIdx = 0; workerIdx < this.dependencyWorkers.size(); ++workerIdx){
			if (this.unassignedTasks.isEmpty()) {
				break; // no more unassigned tasks
			}

		    ActorRef<DependencyWorker.Message> worker = this.dependencyWorkers.get(workerIdx);
		    ActorRef<LargeMessageProxy.Message> workerProxy = this.dependencyWorkerLargeProxies.get(workerIdx);

			if (this.busyWorkers.containsKey(worker)) {
				continue; // this worker is busy
			}
			// this worker is idle

			Task task = this.unassignedTasks.remove();
			String[] headerA = this.headerLines[task.tableA];
			String[] headerB = this.headerLines[task.tableB];
			List<String[]> contentA = this.contentLines.get(task.tableA);
			List<String[]> contentB = this.contentLines.get(task.tableB);

			LargeMessageProxy.LargeMessage taskMessage = new DependencyWorker.TaskMessage(this.largeMessageProxy, headerA, headerB, contentA, contentB);
			this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage (taskMessage, workerProxy));
			this.busyWorkers.put(worker, task);
			this.getContext().getLog().info("Delegated task ({},{})", task.tableA, task.tableB);
		}
	}

	private Behavior<Message> handle(BatchMessage message) {
		this.getContext().getLog().info("Read {} messages from table {}", message.batch.size(), message.id);

		if (message.getBatch().size() != 0) {
			this.inputReaders.get(message.getId()).tell(new InputReader.ReadBatchMessage(this.getContext().getSelf()));
		}
		this.contentLines.get(message.id).addAll(message.batch);

			// generate new tasks for batch message.
		if (message.getBatch().size() == 0) {
			for (int id = 0; id < this.headerLines.length; ++id) {
				// we want to check this new batch against every already-loaded batch
				if (this.finishedReading[id]) {
					this.getContext().getLog().info("Generated task ({},{})", id, message.id);
					this.unassignedTasks.add(new Task(id, message.id));
				}
			}
			this.finishedReading[message.getId()] = true;
		}

		// we may have idle workers
		delegateTasks();

		return this;
	}

	private Behavior<Message> handle(RegistrationMessage message) {
		this.getContext().getLog().info("Registered dependency worker {}", message.dependencyWorker.path());

		ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
		if (!this.dependencyWorkers.contains(dependencyWorker)) {
			this.dependencyWorkers.add(dependencyWorker);
			this.getContext().watch(dependencyWorker);
			this.dependencyWorkerLargeProxies.add(message.getDependencyWorkerLargeMessageProxy());

			// we may have unassigned tasks
			delegateTasks();
		}
		return this;
	}

	private Behavior<Message> handle(CompletionMessage message) {
		ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
		Task task = this.busyWorkers.get(message.getDependencyWorker());
		this.getContext().getLog().info("Completed work for task ({},{})", task.tableA, task.tableB);

		List<InclusionDependency> inds = new ArrayList<>();
		for (ddp.algo.UnaryInclusion.Dependency dep : message.aInB) {
			File dependentFile = this.inputFiles[task.tableA];
			File referencedFile = this.inputFiles[task.tableB];
			String[] dependentColumns = {dep.columnX};
			String[] referencedColumns = {dep.columnY};
			InclusionDependency ind = new InclusionDependency(dependentFile, dependentColumns, referencedFile, referencedColumns);
			inds.add(ind);
		}
		// IMPORTANT: the only thing different here is that A and B are switched (NOT X and Y)
		for (ddp.algo.UnaryInclusion.Dependency dep : message.bInA) {
			File dependentFile = this.inputFiles[task.tableB];
			File referencedFile = this.inputFiles[task.tableA];
			String[] dependentColumns = {dep.columnX};
			String[] referencedColumns = {dep.columnY};
			InclusionDependency ind = new InclusionDependency(dependentFile, dependentColumns, referencedFile, referencedColumns);
			inds.add(ind);
		}

		if (!inds.isEmpty()) {
			this.getContext().getLog().info("Forwarded {} INDs to ResultCollector", inds.size());
			this.resultCollector.tell(new ResultCollector.ResultMessage(inds));
		}

		// I still don't know what task the worker could help me to solve ... but let me keep her busy.
		// Once I found all unary INDs, I could check if this.discoverNaryDependencies is set to true and try to detect n-ary INDs as well!

		// TODO this will throw an error if duplicate CompletionMessages are sent?
		this.busyWorkers.remove(dependencyWorker);

		// we may have unassigned tasks
		delegateTasks();

		// At some point, I am done with the discovery. That is when I should call my end method. Because I do not work on a completable task yet, I simply call it after some time.
		if (System.currentTimeMillis() - this.startTime > 2000000)
			this.end();
		return this;
	}

	private void end() {
		this.resultCollector.tell(new ResultCollector.FinalizeMessage());
		long discoveryTime = System.currentTimeMillis() - this.startTime;
		this.getContext().getLog().info("Finished mining within {} ms!", discoveryTime);
	}

	private Behavior<Message> handle(Terminated signal) {
		ActorRef<DependencyWorker.Message> dependencyWorker = signal.getRef().unsafeUpcast();
		this.dependencyWorkers.remove(dependencyWorker);
		return this;
	}
}