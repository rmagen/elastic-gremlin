package org.elasticgremlin.starQueryHandler.java;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.process.ProcessStandardSuite;
import org.elasticgremlin.starQueryHandler.ModernGraphGraphProvider;
import org.elasticgremlin.structure.ElasticGraph;
import org.junit.runner.RunWith;

@RunWith(ProcessStandardSuite.class)
@GraphProviderClass(provider = ModernGraphGraphProvider.class, graph = ElasticGraph.class)
public class ModernGraphProcessStandardTest {
}
