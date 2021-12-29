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
import de.ddm.actors.profiling.DependencyWorker.TaskMessage;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.InputConfigurationSingleton;
import de.ddm.singletons.SystemConfigurationSingleton;
import de.ddm.structures.InclusionDependency;
import de.ddm.structures.ColumnStorage;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.EqualsAndHashCode;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;

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
		List<String[]> batch; // TODO rename to rows

		public boolean finishedReading(){
			return batch.isEmpty();
		}
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

		List<InclusionDependency> inclusionDependencies;
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
		this.finishedReading = new boolean[this.inputFiles.length];

		this.inputReaders = new ArrayList<>(inputFiles.length);
		for (int id = 0; id < this.inputFiles.length; id++)
			this.inputReaders.add(context.spawn(InputReader.create(id, this.inputFiles[id]), InputReader.DEFAULT_NAME + "_" + id));
		this.resultCollector = context.spawn(ResultCollector.create(), ResultCollector.DEFAULT_NAME);
		this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);

		context.getSystem().receptionist().tell(Receptionist.register(dependencyMinerService, context.getSelf()));
	}

	/////////////////
	// Actor State //
	/////////////////

	private long startTime;

	private final boolean discoverNaryDependencies;
	private final File[] inputFiles;
	private final ColumnStorage dataStorage = new ColumnStorage();
	private final boolean[] finishedReading;

	private final List<ActorRef<InputReader.Message>> inputReaders;
	private final ActorRef<ResultCollector.Message> resultCollector;
	private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;

	// list of all registered DependencyWorkers
	private final List<ActorRef<DependencyWorker.Message>> dependencyWorkers = new ArrayList<>();
	private final List<ActorRef<LargeMessageProxy.Message>> dependencyWorkerLargeProxies = new ArrayList<>();
	// all tasks that not yet assigned
	private final Queue<Task> unassignedTasks = new ArrayDeque<>();
	// all workers that are busy, with their assigned Task
	private final Map<ActorRef<DependencyWorker.Message>, Task> busyWorkers = new HashMap<>();

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
		// ReadHeaderMessage for all input readers
		for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
			inputReader.tell(new InputReader.ReadHeaderMessage(this.getContext().getSelf()));

		this.startTime = System.currentTimeMillis();

		return this;
	}

	private Behavior<Message> handle(HeaderMessage message) {
		this.getContext().getLog().info("Read header of size {} from table {}: {}", message.header.length, message.id, message.header);

		String tableName = this.inputFiles[message.id].getName();
		this.dataStorage.addTable(tableName, Arrays.asList(message.header));

		// first ReadBatchMessage for current input reader
		this.inputReaders.get(message.id).tell(new InputReader.ReadBatchMessage(this.getContext().getSelf()));

		return this;
	}

	// try to delegate all unassigned Tasks to idle DependencyWorkers
	private void delegateTasks(){
		this.getContext().getLog().info("Before task delegation: {} unassigned tasks", this.unassignedTasks.size());

		for (int workerIdx = 0; workerIdx < this.dependencyWorkers.size(); ++workerIdx){
		    ActorRef<DependencyWorker.Message> worker = this.dependencyWorkers.get(workerIdx);
		    ActorRef<LargeMessageProxy.Message> workerProxy = this.dependencyWorkerLargeProxies.get(workerIdx);
			if (this.unassignedTasks.isEmpty()) {
				break; // no more unassigned tasks
			}

			if (this.busyWorkers.containsKey(worker)) {
				continue; // this worker is busy
			}
			// this worker is idle

			Task task = this.unassignedTasks.remove();

			this.getContext().getLog().info("Delegated task {}", task);

			DependencyWorker.TaskMessage taskMessage = new DependencyWorker.TaskMessage(
				this.largeMessageProxy,
				task.tableNameA, task.tableNameB,
				task.columnNamesA, task.columnNamesB,
				task.columnNamesA.stream().map(colName -> this.dataStorage.getColumn(task.tableNameA, colName)).collect(Collectors.toList()),
				task.columnNamesB.stream().map(colName -> this.dataStorage.getColumn(task.tableNameB, colName)).collect(Collectors.toList()));

			this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage (taskMessage, workerProxy));
			this.busyWorkers.put(worker, task);
		}

		this.getContext().getLog().info("After task delegation: {} unassigned tasks", this.unassignedTasks.size());
	}

	// Rough estimate for how much memory space a table column takes up.
	// Of course, an exact calculation is difficult to do in Java.
	private static int columnMemorySize(List<String> column) {
		return column.stream().mapToInt(cell -> cell.length()).sum();
	}

	@AllArgsConstructor
	@Getter
	private static class Task {
		private String tableNameA;
		private String tableNameB;
		private List<String> columnNamesA;
		private List<String> columnNamesB;

		@Override
		public String toString(){
			return String.format("(%s%s,%s%s)", this.tableNameA, this.columnNamesA, this.tableNameB, this.columnNamesB);
		}
	}

	private static class PartialTableTaskGenerator {
		private ColumnStorage dataStorage;
		private final int memoryBudget;

		private String tableNameA;
		private String tableNameB;
		private List<String> headerA;
		private List<String> headerB;

		private List<String> taskHeaderA = new ArrayList<>();
		private List<String> taskHeaderB = new ArrayList<>();
		private List<Task> generatedTasks = new ArrayList<>();

		private PartialTableTaskGenerator(ColumnStorage dataStorage, int memoryBudget, String tableNameA, String tableNameB){
			this.dataStorage = dataStorage;
			this.memoryBudget = memoryBudget;
			this.tableNameA = tableNameA;
			this.tableNameB = tableNameB;
			this.headerA = this.dataStorage.getHeader(tableNameA);
			this.headerB = this.dataStorage.getHeader(tableNameB);
		}

		private void generateTask(){
			this.generatedTasks.add(new Task(
				this.tableNameA, this.tableNameB,
				this.taskHeaderA, this.taskHeaderB));
		}

		private void runTableBGeneration(){
			int memoryUsed = 0;
			for (String columnNameB: this.headerB) {
				List<String> columnB = this.dataStorage.getColumn(tableNameB, columnNameB);
				int columnSizeB = columnMemorySize(columnB);

				// if we exceed memory budget, we want to create partial tables
				if (memoryUsed + columnSizeB > this.memoryBudget / 2) {
					this.generateTask();
					this.taskHeaderB = new ArrayList<>();
					memoryUsed = 0;
				}

				this.taskHeaderB.add(columnNameB);
				// FIXME memory exhaustion
				// this.taskColumnsB.add(columnB);
				memoryUsed += columnSizeB;
			}
			this.generateTask();
		}

		private void runTableAGeneration(){
			int memoryUsed = 0;
			for (String columnNameA: this.headerA) {
				List<String> columnA = this.dataStorage.getColumn(tableNameA, columnNameA);
				int columnSizeA = columnMemorySize(columnA);

				// if we exceed memory budget, we want to create partial tables
				if (memoryUsed + columnSizeA > this.memoryBudget / 2) {
					this.runTableBGeneration();
					this.taskHeaderA = new ArrayList<>();
					memoryUsed = 0;
				}

				this.taskHeaderA.add(columnNameA);
				// FIXME memory exhaustion
				// this.taskColumnsA.add(columnA);
				memoryUsed += columnSizeA;
			}
			this.runTableBGeneration();
		}

		public static List<Task> run(ColumnStorage dataStorage, int memoryBudget, String tableNameA, String tableNameB){
			PartialTableTaskGenerator gen = new PartialTableTaskGenerator(dataStorage, memoryBudget, tableNameA, tableNameB);
			gen.runTableAGeneration();
			return gen.generatedTasks;
		}
	}

	private Behavior<Message> handle(BatchMessage message) {
		String tableName = this.inputFiles[message.id].getName();

		this.getContext().getLog().info("Read {} rows from table {}", message.batch.size(), tableName);

		if (message.finishedReading()) {
			this.finishedReading[message.id] = true;

			// generate new tasks for completed table.
			for (int id = 0; id < this.inputFiles.length; ++id) {
				if (id == message.id) continue; // skip current table

				// we want to check against every other completed table
				if (this.finishedReading[id]) {
					String otherTableName = this.inputFiles[id].getName();
					List<Task> tasks = PartialTableTaskGenerator.run(
						dataStorage,
						100 * 1024 * 1024, // 100 mib
						tableName,
						otherTableName);

					this.getContext().getLog().info("Generated {} tasks for new table {} against table {}", tasks.size(), tableName, otherTableName);
					tasks.forEach(task -> this.getContext().getLog().info("Task: {}", task));

					this.unassignedTasks.addAll(tasks);
				}
			}

			// new tasks available for idle workers
			delegateTasks();
		} else {
			for (String[] row : message.batch) {
				this.dataStorage.addRow(tableName, Arrays.asList(row));
			}

			// follow-up ReadBatchMessage for current input reader
			this.inputReaders.get(message.id).tell(new InputReader.ReadBatchMessage(this.getContext().getSelf()));
		}

		return this;
	}

	private Behavior<Message> handle(RegistrationMessage message) {
		this.getContext().getLog().info("Registered dependency worker {}", message.dependencyWorker.path());

		ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
		if (!this.dependencyWorkers.contains(dependencyWorker)) {
			this.dependencyWorkers.add(dependencyWorker);
			this.getContext().watch(dependencyWorker);
			this.dependencyWorkerLargeProxies.add(message.getDependencyWorkerLargeMessageProxy());

			// new idle workers for unassigned tasks
			delegateTasks();
		}
		return this;
	}

	private Behavior<Message> handle(CompletionMessage message) {
		ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
		Task task = this.busyWorkers.get(message.getDependencyWorker());

		this.getContext().getLog().info("Completed work for task {}", task);

		List<InclusionDependency> inds = message.getInclusionDependencies();
		if (!inds.isEmpty()) {
			this.getContext().getLog().info("Forwarded {} INDs to ResultCollector", inds.size());
			this.resultCollector.tell(new ResultCollector.ResultMessage(inds));
		}

		// Once I found all unary INDs, I could check if this.discoverNaryDependencies is set to true and try to detect n-ary INDs as well!

		// TODO this will throw an error if duplicate CompletionMessages are sent?
		this.busyWorkers.remove(dependencyWorker);

		// we have idle workers for unassigned tasks
		delegateTasks();

		// system finish
		boolean finishedAll = true; // FIXME Arrays.streamBoolean sadly doesnt exist...
		for (boolean b: this.finishedReading){
			if (!b) {
				finishedAll = false;
				break;
			}
		}
		if (finishedAll && this.unassignedTasks.isEmpty() && this.busyWorkers.isEmpty())
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