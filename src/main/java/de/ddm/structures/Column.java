package de.ddm.structures;

import java.util.*;
import java.util.stream.Stream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import de.ddm.serialization.AkkaSerializable;

// Table column which uses optimized skip-list storage
public class Column implements AkkaSerializable {

    // We store unique values in two ways:
    // 1. in an array in order in which they occur
    // 2. in a hashmap with above array position for fast insertion (Array::indexOf gets very slow for large workloads)
    private List<String> valuesByPosition = new ArrayList<>();
    private Map<Integer, Integer> positionByValueHash = new HashMap<>();

    // This is the column data saved as indicies into valuesByPosition instead of their actual values.
    private List<Integer> data = new ArrayList<>();

    // TODO: As further optimization, we may want to store the column data as plain-int arrays instead of Object arrays.
    // private int[] data = new int[128];
    // private int length = 0;

    int memorySize = 0;

    public Column(){}

    public void add(String value){
        int index = this.positionByValueHash.computeIfAbsent(value.hashCode(), _valueHash -> {
            valuesByPosition.add(value);
            this.memorySize += value.length() * 2; // java char takes up 2 bytes
            return valuesByPosition.size() - 1;
        });
        data.add(index);
        this.memorySize += 4; // int takes up 4 bytes (Integer ofc needs more... but for akka serialization, this shouldn't matter?)
    }

    public Stream<String> stream() {
        return data.stream()
            .map(index -> valuesByPosition.get(index));
    }

    public int size() {
        return this.data.size();
    }

    public int getMemorySize(){
        return this.memorySize;
    }

    public Set<String> getUniqueValues() {
        return new HashSet<>(this.valuesByPosition); // TODO can we do this in an more optimzied way?
    }

    @Override
    public String toString(){
        // NOTE this is probably quite costly for large columns... but it shouldn't be used for large columns, anyway.
        return this.stream().toArray().toString();
    }

    /* we implement custom serialization so we don't serialize the redundant valuesWithPosition field */

    // TODO make use of this?
    private static final long serialVersionUID = 7829146444143571165L;

    @SuppressWarnings("unchecked")
    private void readObject(ObjectInputStream input) throws ClassNotFoundException, IOException {       
        this.valuesByPosition = (List<String>) input.readObject();
        this.data = (List<Integer>) input.readObject();
        this.memorySize = input.readInt();

        // TODO re-construct valuesWithPosition... however, if we only want to read, we don't need it?
        //      how about creating an ImmutableColumn class, which doesn't contain it?
    }
 
    private void writeObject(ObjectOutputStream output) throws IOException {
        output.writeObject(this.valuesByPosition);
        output.writeObject(this.data);
        output.writeInt(this.memorySize);
    }
}
