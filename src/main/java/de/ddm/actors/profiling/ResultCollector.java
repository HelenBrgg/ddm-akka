package de.ddm.actors.profiling;

import akka.actor.typed.Behavior;
import akka.actor.typed.PostStop;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.ddm.actors.Guardian;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.DomainConfigurationSingleton;
import de.ddm.structures.InclusionDependency;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import com.opencsv.CSVWriter;
import com.opencsv.ICSVWriter;

public class ResultCollector extends AbstractBehavior<ResultCollector.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable {
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class ResultMessage implements Message {
		private static final long serialVersionUID = -7070569202900845736L;
		List<InclusionDependency> inclusionDependencies;
	}

	@NoArgsConstructor
	public static class FinalizeMessage implements Message {
		private static final long serialVersionUID = -6603856949941810321L;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "resultCollector";

	public static Behavior<Message> create() {
		return Behaviors.setup(ResultCollector::new);
	}

	private ResultCollector(ActorContext<Message> context) throws IOException {
		super(context);

		String outputFileName = DomainConfigurationSingleton.get().getResultCollectorOutputFileName();

		File txt_file = new File(outputFileName + ".txt");
		if (txt_file.exists() && !txt_file.delete())
			throw new IOException("Could not delete existing result file: " + txt_file.getName());
		if (!txt_file.createNewFile())
			throw new IOException("Could not create result file: " + txt_file.getName());

		this.txt_writer = new BufferedWriter(new FileWriter(txt_file));

		File csv_file = new File(outputFileName + ".csv");
		if (csv_file.exists() && !csv_file.delete())
			throw new IOException("Could not delete existing result file: " + csv_file.getName());
		if (!csv_file.createNewFile())
			throw new IOException("Could not create result file: " + csv_file.getName());

		this.csv_writer = new CSVWriter(
			new BufferedWriter(new FileWriter(csv_file)),
			';', // separator
			ICSVWriter.DEFAULT_QUOTE_CHARACTER, ICSVWriter.DEFAULT_ESCAPE_CHARACTER, ICSVWriter.DEFAULT_LINE_END
		);
		// write header
		this.csv_writer.writeNext(new String[]{"Dependent_Table", "Dependent_Attributes", "Referenced_Table", "Referenced_Attributes"});
		this.csv_writer.flush();
	}

	/////////////////
	// Actor State //
	/////////////////

	private final BufferedWriter txt_writer;
	private final CSVWriter csv_writer;

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(ResultMessage.class, this::handle)
				.onMessage(FinalizeMessage.class, this::handle)
				.onSignal(PostStop.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(ResultMessage message) throws IOException {
		this.getContext().getLog().info("Received {} INDs!", message.getInclusionDependencies().size());

		for (InclusionDependency ind : message.getInclusionDependencies()) {
			this.txt_writer.write(ind.toString());
			this.txt_writer.newLine();

			this.csv_writer.writeNext(new String[]{
				ind.getDependentTable(),
				ind.getDependentColumn(),
				ind.getReferencedTable(),
				ind.getReferencedTable()});
		}
		this.txt_writer.flush();
		this.csv_writer.flush();

		return this;
	}

	private Behavior<Message> handle(FinalizeMessage message) throws IOException {
		this.getContext().getLog().info("Received FinalizeMessage!");

		this.txt_writer.flush();
		this.csv_writer.flush();

		this.getContext().getSystem().unsafeUpcast().tell(new Guardian.ShutdownMessage());

		return this;
	}

	private Behavior<Message> handle(PostStop signal) throws IOException {
		this.txt_writer.close();
		this.csv_writer.close();

		return this;
	}
}
