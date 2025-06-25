/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.router.scheduler;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.presto.spi.router.Scheduler;
import com.facebook.presto.spi.router.SchedulerFactory;
import com.google.inject.Injector;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import java.lang.management.ManagementFactory;
import java.util.Map;

import static com.google.common.base.Throwables.throwIfUnchecked;

public class PlanCheckerRouterPluginSchedulerFactory
        implements SchedulerFactory
{
    public static final String PLAN_CHECKER_ROUTER_PLUGIN = "plan-checker";

    @Override
    public String getName()
    {
        return PLAN_CHECKER_ROUTER_PLUGIN;
    }

    @Override
    public Scheduler create(Map<String, String> config)
    {
        try {
            Bootstrap app = new Bootstrap(new PlanCheckerRouterPluginModule());

            Injector injector = app
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();

            try {
                RequestStats stats = injector.getInstance(RequestStats.class);
                ObjectName name = new ObjectName("com.facebook.presto.router.scheduler:type=RequestStats");
                MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

                if (!mbs.isRegistered(name)) {
                    mbs.registerMBean(stats, name);
                }
            }
            catch (InstanceAlreadyExistsException ignored) {
                // ignore if the MBean is already registered
            }

            return injector.getInstance(PlanCheckerRouterPluginScheduler.class);
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}
