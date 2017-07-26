package org.aksw.simba.quetsal.datastructues;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.algebra.Var;

import com.fluidops.fedx.algebra.StatementSource;
/**
 * A hypergraph to represent each DNF groups (BGPs) of triple patterns.
 * @author Saleem
 *
 */
public class HyperGraph {
	/**
	 * Vertex of a hypergraph.
	 * Each vertex has Label, set of incoming hyperedges and outgoing hyperedges
	 * @author Saleem
	 *
	 */
	public static class Vertex {
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((label == null) ? 0 : label.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Vertex other = (Vertex) obj;
			if (label == null) {
				if (other.label != null)
					return false;
			} else if (!label.equals(other.label))
				return false;
			return true;
		}
		public final Var var;
		public final String label;
		public final Set<HyperEdge> inEdges;
		public final Set<HyperEdge> outEdges;
		/**
		 *  Vertex constructor 
		 * @param label Label of a vertex
		 */
		public Vertex(Var val, String label) {
			this.var = val;
			this.label = label;
			inEdges = new HashSet<HyperEdge>();
			outEdges = new HashSet<HyperEdge>();
		}

		@Override
		public String toString() {
			return label;
		}
	}
	
	/**
	 * A hyperedge of a hypergraph. Each hyper edge contains subject node as head and predicate, objects nodes as tail of that hyper edge.
	 * Label of a hyperedge contains the relevant source set
	 * @author Saleem
	 *
	 */
	public static class HyperEdge{
		public final Vertex subj;
		public final Vertex pred;
		public final Vertex obj;
		public final List<StatementSource> label;
		/**
		 * A hyperedge constructor. 
		 * @param subject Subject vertex and head of the hyperedge
		 * @param predicate Predicate Vertex
		 * @param object Object Vertesx
		 */
		public HyperEdge(Vertex subject, Vertex predicate, Vertex object) {
			this.subj= subject;
			this.pred = predicate;
			this.obj = object;
			this.label = new ArrayList<StatementSource> ();
		}
	}
}