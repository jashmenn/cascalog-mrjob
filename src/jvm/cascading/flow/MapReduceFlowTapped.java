/*
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Cascalog.  If not, see <http://www.gnu.org/licenses/>.
 */

package cascading.flow;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import cascading.flow.StepGraph;
import cascading.flow.FlowStep;
import cascading.flow.MapReduceFlowStep;
import cascading.scheme.Scheme;
import cascading.tap.Hfs;
import cascading.tap.Tap;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.log4j.Logger;

/**
 * Class MapReduceFlow is a {@link Flow} subclass that supports custom MapReduce jobs preconfigured via the {@link JobConf}
 * object.
 * <p/>
 * Use this class to allow custom MapReduce jobs to participage in the {@link cascading.cascade.Cascade} scheduler. If
 * other Flow instances in the Cascade share resources with this Flow instance, all participants will be scheduled
 * according to their dependencies (topologically).
 * <p/>
 * Set the parameter {@code deleteSinkOnInit} to {@code true} if the outputPath in the jobConf should be deleted before executing the MapReduce job.
 */
public class MapReduceFlowTapped extends cascading.flow.MapReduceFlow
  {
  /** Field LOG */
  private static final Logger LOG = Logger.getLogger( MapReduceFlowTapped.class );

  /** Field deleteSinkOnInit */
  private boolean deleteSinkOnInit = false;

  /**
   * Constructor MapReduceFlowTapped creates a new MapReduceFlowTapped instance. Allows you
   * to set your own Taps.
   *
   * @param name             of type String
   * @param jobConf          of type JobConf
   * @param source           of type Tap
   * @param sink             of type Tap
   * @param deleteSinkOnInit of type boolean
   * @param stopJobsOnExit   of type boolean
   */
  public MapReduceFlowTapped( String name, JobConf jobConf, Tap source, Tap sink, boolean deleteSinkOnInit, boolean stopJobsOnExit )
    {
      this( name, jobConf, null, null, null, deleteSinkOnInit, stopJobsOnExit );

      Map<String, Tap> sources = new HashMap<String, Tap>();
      Map<String, Tap> sinks   = new HashMap<String, Tap>();
      Map<String, Tap> traps   = new HashMap<String, Tap>();

      sources.put( source.getPath().toString(), source );
      sinks.put( sink.getPath().toString(), sink );

      setSources( sources );
      setSinks( sinks );
      setTraps( traps );
      setStepGraph( makeStepGraph( jobConf ) );
    }

  /**
   * Constructor MapReduceFlowTapped creates a new MapReduceFlow instance.
   *
   * @param name             of type String
   * @param jobConf          of type JobConf
   * @param sources          of type Map<String, Tap>
   * @param sinks            of type Map<String, Tap>
   * @param traps            of type Map<String, Tap>
   * @param deleteSinkOnInit of type boolean
   * @param stopJobsOnExit   of type boolean
   */
  public MapReduceFlowTapped( String name, JobConf jobConf, Map<String, Tap> sources, Map<String, Tap> sinks, Map<String, Tap> traps, 
      boolean deleteSinkOnInit, boolean stopJobsOnExit )
    {
    super( jobConf );
    this.deleteSinkOnInit = deleteSinkOnInit;
    this.stopJobsOnExit = stopJobsOnExit;

    setName( name );

    if( sources != null )
      setSources( sources );

    if( sinks != null )
      setSinks( sinks );

    if( traps != null )
      setTraps( traps );

    if( (sources != null) && (sinks != null) && (traps != null) )
      setStepGraph( makeStepGraph( jobConf ) );
    }

  private StepGraph makeStepGraph( JobConf jobConf )
    {
    StepGraph stepGraph = new StepGraph();

    Tap sink = getSinks().values().iterator().next();
    FlowStep step = new MapReduceFlowStep( sink.toString(), jobConf, sink );

    step.setParentFlowName( getName() );

    stepGraph.addVertex( step );

    return stepGraph;
    }


  }
