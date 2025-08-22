def _log_final_stats(self):
        """Log final system statistics"""
        uptime = time.time() - self.system_stats['start_time']
        total_requests = self.system_stats['total_requests']
        success_rate = (self.system_stats['successful_requests'] / max(total_requests, 1)) * 100
        
        if total_requests > 0:
            avg_response_time = self.system_stats['total_response_time_us'] / total_requests / 1000  # ms
            requests_per_second = total_requests / max(uptime, 1)
            
            logging.info("=== FINAL SYSTEM STATISTICS ===")
            logging.info(f"Total uptime: {uptime:.1f} seconds")
            logging.info(f"Total requests processed: {total_requests}")
            logging.info(f"Successful requests: {self.system_stats['successful_requests']}")
            logging.info(f"Failed requests: {self.system_stats['failed_requests']}")
            logging.info(f"Success rate: {success_rate:.2f}%")
            logging.info(f"Average response time: {avg_response_time:.2f}ms")
            logging.info(f"Throughput: {requests_per_second:.1f} requests/second")
            logging.info(f"Workers deployed: {len(self.workers)}")
    
    def read_tag(self, plc_ip: str, tag_name: str, timeout: float = 5.0) -> Optional[Any]:
        """Ultra-fast single tag read with comprehensive error handling"""
        if not self.running:
            logging.error("System not running")
            return None
        
        request_id = self._generate_request_id("read")
        
        request = EIPRequest(
            request_id=request_id,
            plc_ip=plc_ip,
            operation='read',
            tag_name=tag_name,
            timeout_ms=int(timeout * 1000)
        )
        
        if not request.validate():
            logging.error(f"Invalid read request: {request}")
            return None
        
        response = self._send_request_and_wait(request, timeout)
        return response.value if response and response.success else None
    
    def write_tag(self, plc_ip: str, tag_name: str, value: Any, 
                  data_type: int = EIPDataType.DINT, timeout: float = 5.0) -> bool:
        """Ultra-fast single tag write with validation"""
        if not self.running:
            logging.error("System not running")
            return False
        
        request_id = self._generate_request_id("write")
        
        request = EIPRequest(
            request_id=request_id,
            plc_ip=plc_ip,
            operation='write',
            tag_name=tag_name,
            value=value,
            data_type=data_type,
            timeout_ms=int(timeout * 1000)
        )
        
        if not request.validate():
            logging.error(f"Invalid write request: {request}")
            return False
        
        response = self._send_request_and_wait(request, timeout)
        return response is not None and response.success
    
    def read_tags_batch(self, plc_ip: str, tag_names: List[str], timeout: float = 10.0) -> List[Any]:
        """ULTRA HIGH PERFORMANCE batch read with validation"""
        if not self.running:
            logging.error("System not running")
            return [None] * len(tag_names)
        
        if not tag_names:
            logging.error("No tag names provided for batch read")
            return []
        
        # Validate tag names
        if len(tag_names) > 100:  # Safety limit
            logging.warning(f"Batch size {len(tag_names)} exceeds recommended limit, may cause timeouts")
        
        request_id = self._generate_request_id("batch")
        
        request = EIPRequest(
            request_id=request_id,
            plc_ip=plc_ip,
            operation='read_batch',
            tag_names=tag_names,
            timeout_ms=int(timeout * 1000)
        )
        
        if not request.validate():
            logging.error(f"Invalid batch read request: {request}")
            return [None] * len(tag_names)
        
        response = self._send_request_and_wait(request, timeout)
        if response and response.success and response.values:
            return response.values
        else:
            return [None] * len(tag_names)
    
    def read_udt_tag(self, plc_ip: str, tag_name: str, udt_name: str, timeout: float = 5.0) -> Optional[Dict[str, Any]]:
        """Read UDT tag and return structured data"""
        if not self.running:
            logging.error("System not running")
            return None
        
        request_id = self._generate_request_id("read_udt")
        
        request = EIPRequest(
            request_id=request_id,
            plc_ip=plc_ip,
            operation='read_udt',
            tag_name=tag_name,
            udt_name=udt_name,
            timeout_ms=int(timeout * 1000)
        )
        
        if not request.validate():
            logging.error(f"Invalid UDT read request: {request}")
            return None
        
        response = self._send_request_and_wait(request, timeout)
        return response.udt_data if response and response.success else None
    
    def write_udt_tag(self, plc_ip: str, tag_name: str, udt_name: str, 
                      udt_data: Dict[str, Any], timeout: float = 5.0) -> bool:
        """Write structured data to UDT tag"""
        if not self.running:
            logging.error("System not running")
            return False
        
        request_id = self._generate_request_id("write_udt")
        
        request = EIPRequest(
            request_id=request_id,
            plc_ip=plc_ip,
            operation='write_udt',
            tag_name=tag_name,
            udt_name=udt_name,
            udt_data=udt_data,
            timeout_ms=int(timeout * 1000)
        )
        
        if not request.validate():
            logging.error(f"Invalid UDT write request: {request}")
            return False
        
        response = self._send_request_and_wait(request, timeout)
        return response is not None and response.success
    
    def get_udt_definition(self, plc_ip: str, udt_name: str, timeout: float = 5.0) -> Optional[Dict[str, Any]]:
        """Get UDT structure definition"""
        if not self.running:
            logging.error("System not running")
            return None
        
        request_id = self._generate_request_id("udt_def")
        
        request = EIPRequest(
            request_id=request_id,
            plc_ip=plc_ip,
            operation='get_udt_definition',
            udt_name=udt_name,
            timeout_ms=int(timeout * 1000)
        )
        
        if not request.validate():
            logging.error(f"Invalid UDT definition request: {request}")
            return None
        
        response = self._send_request_and_wait(request, timeout)
        return response.udt_definition if response and response.success else None
    
    def read_multiple_plcs(self, requests: List[Dict], timeout: float = 10.0) -> List[Any]:
        """Simultaneously read from multiple PLCs with comprehensive error handling"""
        if not self.running:
            logging.error("System not running")
            return [None] * len(requests)
        
        if not requests:
            logging.warning("No requests provided for multi-PLC read")
            return []
        
        # Validate all requests first
        for i, req in enumerate(requests):
            if not isinstance(req, dict):
                logging.error(f"Request {i} is not a dictionary")
                return [None] * len(requests)
            
            required_fields = ['plc_ip', 'tag_name']
            if not all(field in req for field in required_fields):
                logging.error(f"Request {i} missing required fields: {required_fields}")
                return [None] * len(requests)
        
        request_ids = []
        queues = {}
        
        try:
            # Send all requests
            for i, req in enumerate(requests):
                request_id = self._generate_request_id(f"multi_{i}")
                request_ids.append(request_id)
                
                eip_request = EIPRequest(
                    request_id=request_id,
                    plc_ip=req['plc_ip'],
                    operation='read',
                    tag_name=req['tag_name'],
                    timeout_ms=int(timeout * 1000)
                )
                
                if not eip_request.validate():
                    logging.error(f"Invalid request {i}: {eip_request}")
                    continue
                
                queue = Queue()
                queues[request_id] = queue
                self.pending_requests[request_id] = queue
                
                try:
                    self.ventilator.send_json(asdict(eip_request), zmq.NOBLOCK)
                except zmq.Again:
                    logging.error(f"Failed to send request {i} - queue full")
                    self.pending_requests.pop(request_id, None)
                    continue
            
            # Collect results with individual timeouts
            results = []
            start_time = time.time()
            
            for request_id in request_ids:
                if request_id not in queues:
                    results.append(None)
                    continue
                
                remaining_time = max(0.1, timeout - (time.time() - start_time))
                
                try:
                    response = queues[request_id].get(timeout=remaining_time)
                    results.append(response.value if response.success else None)
                except Empty:
                    logging.warning(f"Timeout waiting for response {request_id}")
                    results.append(None)
                except Exception as e:
                    logging.error(f"Error getting response {request_id}: {e}")
                    results.append(None)
            
            return results
            
        except Exception as e:
            logging.error(f"Critical error in multi-PLC read: {e}")
            return [None] * len(requests)
        finally:
            # Always cleanup pending requests
            for request_id in request_ids:
                self.pending_requests.pop(request_id, None)
    
    def get_system_status(self) -> Dict[str, Any]:
        """Get comprehensive system status from all components"""
        if not self.running:
            return {'error': 'System not running'}
        
        try:
            # Get current system stats
            current_stats = self.system_stats.copy()
            current_stats['queue_depth'] = len(self.pending_requests)
            current_stats['uptime_seconds'] = time.time() - self.system_stats['start_time']
            
            # Calculate rates
            if current_stats['total_requests'] > 0:
                current_stats['success_rate_percent'] = (
                    current_stats['successful_requests'] / current_stats['total_requests'] * 100
                )
                current_stats['avg_response_time_ms'] = (
                    current_stats['total_response_time_us'] / current_stats['total_requests'] / 1000
                )
                current_stats['requests_per_second'] = (
                    current_stats['total_requests'] / max(current_stats['uptime_seconds'], 1)
                )
            
            # Get worker status (sample from first available worker)
            worker_status = self._get_worker_status_sample()
            
            system_status = {
                'system': current_stats,
                'workers': {
                    'count': len(self.workers),
                    'active': len([w for w in self.workers if w.is_alive()]),
                    'sample_status': worker_status
                },
                'zmq': {
                    'ventilator_port': self.ventilator_port,
                    'sink_port': self.sink_port,
                    'pending_requests': len(self.pending_requests),
                    'high_water_mark': self.config.zmq_high_water_mark
                },
                'config': asdict(self.config),
                'timestamp': time.time()
            }
            
            return system_status
            
        except Exception as e:
            logging.error(f"Error getting system status: {e}")
            return {'error': f'Failed to get status: {str(e)}'}
    
    def _get_worker_status_sample(self) -> Optional[Dict[str, Any]]:
        """Get status from one worker as a representative sample"""
        try:
            # Send status request to any available worker
            request_id = self._generate_request_id("status")
            
            status_request = EIPRequest(
                request_id=request_id,
                plc_ip="STATUS_REQUEST",  # Special marker
                operation='status',
                timeout_ms=3000
            )
            
            queue = Queue()
            self.pending_requests[request_id] = queue
            
            try:
                self.ventilator.send_json(asdict(status_request), zmq.NOBLOCK)
                response = queue.get(timeout=3.0)
                
                if response.success and response.value:
                    return response.value
                    
            except (zmq.Again, Empty):
                pass
            finally:
                self.pending_requests.pop(request_id, None)
                
        except Exception as e:
            logging.error(f"Error getting worker status sample: {e}")
        
        return None
    
    def benchmark_performance(self, plc_ip: str, tag_name: str, iterations: int = 1000) -> Dict[str, float]:
        """Comprehensive performance benchmark"""
        if not self.running:
            return {'error': 'System not running'}
        
        logging.info(f"Starting performance benchmark: {iterations} iterations on {plc_ip}/{tag_name}")
        
        start_time = time.perf_counter()
        successful_reads = 0
        failed_reads = 0
        total_response_time = 0
        response_times = []
        
        for i in range(iterations):
            iter_start = time.perf_counter()
            
            try:
                value = self.read_tag(plc_ip, tag_name, timeout=5.0)
                iter_time = (time.perf_counter() - iter_start) * 1000  # ms
                
                if value is not None:
                    successful_reads += 1
                    total_response_time += iter_time
                    response_times.append(iter_time)
                else:
                    failed_reads += 1
                    
            except Exception as e:
                logging.error(f"Benchmark iteration {i} failed: {e}")
                failed_reads += 1
            
            # Progress logging
            if (i + 1) % 100 == 0:
                logging.info(f"Benchmark progress: {i + 1}/{iterations}")
        
        total_time = time.perf_counter() - start_time
        
        # Calculate statistics
        results = {
            'total_time_seconds': total_time,
            'iterations': iterations,
            'successful_reads': successful_reads,
            'failed_reads': failed_reads,
            'success_rate_percent': (successful_reads / iterations) * 100,
            'tags_per_second': successful_reads / total_time,
            'requests_per_second': iterations / total_time,
            'total_response_time_ms': total_response_time,
        }
        
        if response_times:
            response_times.sort()
            results.update({
                'avg_response_time_ms': sum(response_times) / len(response_times),
                'min_response_time_ms': min(response_times),
                'max_response_time_ms': max(response_times),
                'p50_response_time_ms': response_times[len(response_times) // 2],
                'p95_response_time_ms': response_times[int(len(response_times) * 0.95)],
                'p99_response_time_ms': response_times[int(len(response_times) * 0.99)],
            })
        
        logging.info(f"Benchmark complete: {results['tags_per_second']:.0f} tags/sec, "
                    f"{results['success_rate_percent']:.1f}% success rate")
        
        return results
    
    def stress_test_batch(self, plc_ip: str, tag_names: List[str], iterations: int = 100) -> Dict[str, float]:
        """Comprehensive batch stress test"""
        if not self.running:
            return {'error': 'System not running'}
        
        logging.info(f"Starting batch stress test: {iterations} iterations of {len(tag_names)} tags")
        
        start_time = time.perf_counter()
        successful_batches = 0
        failed_batches = 0
        total_tags_read = 0
        total_tags_attempted = iterations * len(tag_names)
        batch_times = []
        
        for i in range(iterations):
            batch_start = time.perf_counter()
            
            try:
                values = self.read_tags_batch(plc_ip, tag_names, timeout=15.0)
                batch_time = (time.perf_counter() - batch_start) * 1000  # ms
                
                if values and len(values) == len(tag_names):
                    successful_batches += 1
                    successful_tags = len([v for v in values if v is not None])
                    total_tags_read += successful_tags
                    batch_times.append(batch_time)
                else:
                    failed_batches += 1
                    
            except Exception as e:
                logging.error(f"Batch stress test iteration {i} failed: {e}")
                failed_batches += 1
            
            # Progress logging
            if (i + 1) % 10 == 0:
                logging.info(f"Stress test progress: {i + 1}/{iterations}")
        
        total_time = time.perf_counter() - start_time
        
        # Calculate results
        results = {
            'total_time_seconds': total_time,
            'batch_iterations': iterations,
            'successful_batches': successful_batches,
            'failed_batches': failed_batches,
            'batch_success_rate_percent': (successful_batches / iterations) * 100,
            'total_tags_attempted': total_tags_attempted,
            'total_tags_read': total_tags_read,
            'tag_success_rate_percent': (total_tags_read / total_tags_attempted) * 100,
            'tags_per_second': total_tags_read / total_time,
            'batches_per_second': successful_batches / total_time,
        }
        
        if batch_times:
            batch_times.sort()
            results.update({
                'avg_batch_time_ms': sum(batch_times) / len(batch_times),
                'min_batch_time_ms': min(batch_times),
                'max_batch_time_ms': max(batch_times),
                'p95_batch_time_ms': batch_times[int(len(batch_times) * 0.95)],
            })
        
        logging.info(f"Stress test complete: {results['tags_per_second']:.0f} tags/sec, "
                    f"{results['batch_success_rate_percent']:.1f}% batch success rate")
        
        return results
    
    def _generate_request_id(self, operation: str) -> str:
        """Generate unique request ID"""
        self.request_counter += 1
        timestamp = int(time.time() * 1000000)  # microseconds
        return f"{operation}_{self.request_counter}_{timestamp}"
    
    def _send_request_and_wait(self, request: EIPRequest, timeout: float) -> Optional[EIPResponse]:
        """Send request and wait for response with error handling"""
        result_queue = Queue()
        self.pending_requests[request.request_id] = result_queue
        
        try:
            # Send request
            self.ventilator.send_json(asdict(request), zmq.NOBLOCK)
            
            # Update system stats
            self.system_stats['total_requests'] += 1
            
            # Wait for response
            try:
                response = result_queue.get(timeout=timeout)
                
                # Update response stats
                if response.success:
                    self.system_stats['successful_requests'] += 1
                else:
                    self.system_stats['failed_requests'] += 1
                
                if response.response_time_us > 0:
                    self.system_stats['total_response_time_us'] += response.response_time_us
                
                return response
                
            except Empty:
                logging.warning(f"Timeout waiting for response to {request.request_id}")
                self.system_stats['failed_requests'] += 1
                return None
                
        except zmq.Again:
            logging.error(f"Failed to send request {request.request_id} - queue full")
            self.system_stats['failed_requests'] += 1
            return None
        except Exception as e:
            logging.error(f"Error sending request {request.request_id}: {e}")
            self.system_stats['failed_requests'] += 1
            return None
        finally:
            self.pending_requests.pop(request.request_id, None)
    
    def _collect_responses(self):
        """Background thread that collects responses from workers"""
        logging.info("Response collector thread started")
        
        while self.running and not self.shutdown_event.is_set():
            try:
                if self.sink.poll(timeout=1000):  # 1 second timeout
                    message = self.sink.recv_json(zmq.NOBLOCK)
                    response = EIPResponse(**message)
                    
                    # Route to waiting request
                    if response.request_id in self.pending_requests:
                        queue = self.pending_requests[response.request_id]
                        try:
                            queue.put(response, timeout=0.1)
                        except:
                            # Queue full or request timed out - not critical
                            pass
                    else:
                        # Orphaned response - request may have timed out
                        logging.debug(f"Received orphaned response: {response.request_id}")
                        
            except zmq.Again:
                continue  # No message available
            except Exception as e:
                if self.running:  # Only log if we're not shutting down
                    logging.error(f"Error in response collector: {e}")
        
        logging.info("Response collector thread stopped")
    
    def _system_monitor(self):
        """Background thread for system monitoring and health checks"""
        logging.info("System monitor thread started")
        
        last_stats_log = 0
        stats_interval = 300  # Log stats every 5 minutes
        
        while self.running and not self.shutdown_event.is_set():
            try:
                current_time = time.time()
                
                # Log periodic statistics
                if current_time - last_stats_log >= stats_interval:
                    self._log_periodic_stats()
                    last_stats_log = current_time
                
                # Check worker health
                self._check_worker_health()
                
                # Update queue depth
                self.system_stats['queue_depth'] = len(self.pending_requests)
                
                # Sleep before next check
                time.sleep(10)  # Check every 10 seconds
                
            except Exception as e:
                if self.running:
                    logging.error(f"Error in system monitor: {e}")
        
        logging.info("System monitor thread stopped")
    
    def _log_periodic_stats(self):
        """Log periodic system statistics"""
        try:
            uptime = time.time() - self.system_stats['start_time']
            total_requests = self.system_stats['total_requests']
            
            if total_requests > 0:
                success_rate = (self.system_stats['successful_requests'] / total_requests) * 100
                avg_response_time = self.system_stats['total_response_time_us'] / total_requests / 1000
                requests_per_second = total_requests / uptime
                
                logging.info("=== PERIODIC SYSTEM STATS ===")
                logging.info(f"Uptime: {uptime:.0f}s, Requests: {total_requests}, "
                           f"Success: {success_rate:.1f}%, Avg response: {avg_response_time:.2f}ms, "
                           f"Rate: {requests_per_second:.1f} req/s, Queue: {len(self.pending_requests)}")
            
        except Exception as e:
            logging.error(f"Error logging periodic stats: {e}")
    
    def _check_worker_health(self):
        """Check health of all worker processes"""
        try:
            dead_workers = []
            for i, worker in enumerate(self.workers):
                if not worker.is_alive():
                    logging.error(f"Worker {i} is dead!")
                    dead_workers.append(i)
            
            if dead_workers:
                logging.error(f"Dead workers detected: {dead_workers}")
                # In production, you might want to restart dead workers here
            
            self.system_stats['active_workers'] = len(self.workers) - len(dead_workers)
            
        except Exception as e:
            logging.error(f"Error checking worker health: {e}")

# =============================================================================
# Example Usage and Testing
# =============================================================================

def create_libeip_stub():
    """Create a stub libeip.so for testing when real library isn't available"""
    stub_code = '''
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>

// Stub implementation for testing
typedef struct {
    unsigned int session_handle;
    unsigned int connection_id;
    int is_connected;
    int last_error;
    unsigned int response_time_us;
} EIPSessionInfo;

typedef struct {
    unsigned short data_type;
    unsigned short element_count;
    unsigned int data_size;
    unsigned int max_data_size;
    unsigned char data[4096];
} SafeEIPTagData;

void* eip_session_create(const char* host, unsigned short port) {
    printf("STUB: Creating session for %s:%d\\n", host, port);
    return (void*)0x12345678;  // Fake handle
}

void eip_session_destroy(void* session) {
    printf("STUB: Destroying session\\n");
}

int eip_session_connect(void* session) {
    printf("STUB: Connecting session\\n");
    usleep(1000);  // Simulate connection time
    return 0;  // Success
}

void eip_session_disconnect(void* session) {
    printf("STUB: Disconnecting session\\n");
}

int eip_session_get_info(void* session, EIPSessionInfo* info) {
    info->session_handle = 0x12345678;
    info->connection_id = 0x87654321;
    info->is_connected = 1;
    info->last_error = 0;
    info->response_time_us = 1000;
    return 0;
}

int eip_read_tag(void* session, const char* tag_name, SafeEIPTagData* tag_data) {
    printf("STUB: Reading tag %s\\n", tag_name);
    
    // Return fake DINT value
    tag_data->data_type = 0x00C8;  // DINT
    tag_data->element_count = 1;
    tag_data->data_size = 4;
    tag_data->max_data_size = 4096;
    
    // Generate pseudo-random value based on tag name
    int value = 42;
    for (int i = 0; tag_name[i]; i++) {
        value += tag_name[i] * (i + 1);
    }
    *((int*)tag_data->data) = value;
    
    usleep(500);  // Simulate 0.5ms response time
    return 0;
}

int eip_write_tag(void* session, const char* tag_name, unsigned short data_type, 
                  void* data, unsigned int data_size) {
    printf("STUB: Writing tag %s (type=%d, size=%d)\\n", tag_name, data_type, data_size);
    usleep(500);  // Simulate 0.5ms response time
    return 0;
}

int eip_read_tag_list(void* session, char** tag_names, SafeEIPTagData* tag_data_array, 
                      unsigned int count) {
    printf("STUB: Reading %d tags in batch\\n", count);
    
    for (unsigned int i = 0; i < count; i++) {
        tag_data_array[i].data_type = 0x00C8;  // DINT
        tag_data_array[i].element_count = 1;
        tag_data_array[i].data_size = 4;
        tag_data_array[i].max_data_size = 4096;
        *((int*)tag_data_array[i].data) = 42 + i;  // Fake values
    }
    
    usleep(count * 100);  // Simulate batch time
    return 0;
}

int eip_send_heartbeat(void* session) {
    printf("STUB: Sending heartbeat\\n");
    usleep(100);  // Simulate 0.1ms heartbeat
    return 0;
}

int eip_get_connection_stats(void* session, unsigned int* packets_sent,
                            unsigned int* packets_received, unsigned int* errors,
                            unsigned int* avg_response_time_us) {
    *packets_sent = 1000;
    *packets_received = 995;
    *errors = 5;
    *avg_response_time_us = 800;
    return 0;
}

// UDT stub functions
int eip_read_udt_definition(void* session, const char* udt_name, void* udt_definition) {
    printf("STUB: Reading UDT definition for %s\\n", udt_name);
    usleep(1000);
    return 0;  // Success
}

int eip_read_udt_tag(void* session, const char* tag_name, const char* udt_name, 
                     SafeEIPTagData* tag_data) {
    printf("STUB: Reading UDT tag %s (type %s)\\n", tag_name, udt_name);
    tag_data->data_type = 0x02A0;  // UDT
    tag_data->element_count = 1;
    tag_data->data_size = 100;  // Fake UDT data
    tag_data->max_data_size = 4096;
    usleep(1000);
    return 0;
}

int eip_write_udt_tag(void* session, const char* tag_name, const char* udt_name,
                      void* data, unsigned int data_size) {
    printf("STUB: Writing UDT tag %s (type %s, size %d)\\n", tag_name, udt_name, data_size);
    usleep(1000);
    return 0;
}

// Optional logging functions for testing
void eip_set_log_level(int level) {
    printf("STUB: Log level set to %d\\n", level);
}

void eip_enable_debug(int enable) {
    printf("STUB: Debug logging %s\\n", enable ? "enabled" : "disabled");
}
'''
    
    try:
        with open('libeip_stub.c', 'w') as f:
            f.write(stub_code)
        
        print("Created libeip_stub.c")
        print("Compile with: gcc -shared -fPIC -o libeip.so libeip_stub.c")
        print("Then run: python industrial_plc_system.py")
        
    except Exception as e:
        print(f"Error creating stub: {e}")

def main():
    """Example usage of the industrial-grade PLC system"""
    # Setup comprehensive logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('plc_system.log')
        ]
    )
    
    # Create configuration
    config = EIPConfig(
        worker_count=4,
        max_packet_size=4096,
        connection_timeout_ms=5000,
        heartbeat_interval_sec=30,
        max_retries=5,
        circuit_breaker_threshold=10,
        zmq_high_water_mark=1000
    )
    
    # Create industrial-grade EIP manager
    eip_manager = IndustrialZMQEIPManager(
        config=config,
        libeip_path="./libeip.so"
    )
    
    try:
        # Start the system
        eip_manager.start()
        
        print("=== Industrial-Grade ZMQ + libeip PLC Communication System ===")
        print(f"Configuration: {config}")
        print()
        
        # Example 1: Single tag operations with error handling
        print("1. Single Tag Operations:")
        try:
            value = eip_manager.read_tag('192.168.1.100', 'ProductionCount')
            print(f"   Read ProductionCount: {value}")
            
            success = eip_manager.write_tag('192.168.1.100', 'SetPoint', 1500, EIPDataType.DINT)
            print(f"   Write SetPoint: {'Success' if success else 'Failed'}")
            
            value = eip_manager.read_tag('192.168.1.100', 'SetPoint')
            print(f"   Read SetPoint after write: {value}")
            
        except Exception as e:
            print(f"   Error in single tag operations: {e}")
        print()
        
        # Example 2: High-performance batch operations
        print("2. High-Performance Batch Operations:")
        try:
            # Read 50 tags in one batch
            tag_names = [f'LineSpeed_{i}' for i in range(1, 51)]
            start_time = time.perf_counter()
            values = eip_manager.read_tags_batch('192.168.1.100', tag_names)
            batch_time = time.perf_counter() - start_time
            
            successful_reads = len([v for v in values if v is not None]) if values else 0
            print(f"   Read {successful_reads}/{len(tag_names)} tags in {batch_time:.3f}s")
            print(f"   Performance: {successful_reads/batch_time:.0f} tags/second")
            
            # Show sample values
            if values:
                sample_values = [(tag_names[i], values[i]) for i in range(min(5, len(values))) if values[i] is not None]
                print(f"   Sample values: {sample_values}")
                
        except Exception as e:
            print(f"   Error in batch operations: {e}")
        print()
        
        # Example 3: Multiple PLC operations (distributed manufacturing)
        print("3. Multi-PLC Distributed Operations:")
        try:
            multi_requests = [
                {'plc_ip': '192.168.1.100', 'tag_name': 'Line1_Production'},
                {'plc_ip': '192.168.1.101', 'tag_name': 'Line2_Production'},
                {'plc_ip': '192.168.1.102', 'tag_name': 'Line3_Production'},
                {'plc_ip': '192.168.1.103', 'tag_name': 'Line4_Production'},
                {'plc_ip': '192.168.1.104', 'tag_name': 'QualityStation_Status'},
            ]
            
            start_time = time.perf_counter()
            multi_results = eip_manager.read_multiple_plcs(multi_requests)
            multi_time = time.perf_counter() - start_time
            
            print(f"   Read from {len(multi_requests)} PLCs in {multi_time:.3f}s")
            for i, result in enumerate(multi_results):
                req = multi_requests[i]
                status = "✓" if result is not None else "✗"
                print(f"   {status} {req['plc_ip']}/{req['tag_name']}: {result}")
                
        except Exception as e:
            print(f"   Error in multi-PLC operations: {e}")
        print()
        
        # Example 4: UDT (User Defined Type) operations
        print("4. UDT (User Defined Type) Operations:")
        try:
            # Get UDT definition
            udt_def = eip_manager.get_udt_definition('192.168.1.100', 'RecipeData')
            if udt_def:
                print(f"   UDT Definition: {udt_def['name']} ({udt_def['size']} bytes, {udt_def['member_count']} members)")
                for member in udt_def['members'][:3]:  # Show first 3 members
                    print(f"     - {member['name']}: {member['data_type']} @ offset {member['offset']}")
            
            # Read UDT tag
            udt_data = eip_manager.read_udt_tag('192.168.1.100', 'CurrentRecipe', 'RecipeData')
            if udt_data:
                print(f"   UDT Data keys: {list(udt_data.keys())[:5]}")  # Show first 5 keys
            
            # Write UDT tag
            new_recipe = {
                'Temperature': 350.5,
                'Pressure': 15.2,
                'Duration': 120,
                'QualityLevel': 'A'
            }
            success = eip_manager.write_udt_tag('192.168.1.100', 'NextRecipe', 'RecipeData', new_recipe)
            print(f"   Write UDT: {'Success' if success else 'Failed'}")
            
        except Exception as e:
            print(f"   Error in UDT operations: {e}")
        print()
        
        # Example 5: Comprehensive system status
        print("5. System Health and Status:")
        try:
            status = eip_manager.get_system_status()
            
            if 'error' not in status:
                sys_stats = status['system']
                worker_stats = status['workers']
                
                print(f"   System Uptime: {sys_stats.get('uptime_seconds', 0):.0f} seconds")
                print(f"   Total Requests: {sys_stats.get('total_requests', 0)}")
                print(f"   Success Rate: {sys_stats.get('success_rate_percent', 0):.1f}%")
                print(f"   Avg Response Time: {sys_stats.get('avg_response_time_ms', 0):.2f}ms")
                print(f"   Current Queue Depth: {sys_stats.get('queue_depth', 0)}")
                print(f"   Active Workers: {worker_stats.get('active', 0)}/{worker_stats.get('count', 0)}")
                
                # Show worker sample if available
                if worker_stats.get('sample_status'):
                    sample = worker_stats['sample_status']
                    if 'worker_stats' in sample:
                        ws = sample['worker_stats']
                        print(f"   Worker Sample: {ws.get('requests_processed', 0)} processed, "
                              f"{ws.get('reconnect_attempts', 0)} reconnect attempts")
            else:
                print(f"   Status Error: {status['error']}")
                
        except Exception as e:
            print(f"   Error getting system status: {e}")
        print()
        
        # Example 6: Performance benchmarking
        print("6. Performance Benchmark:")
        try:
            benchmark = eip_manager.benchmark_performance(
                '192.168.1.100', 'BenchmarkTag', iterations=500
            )
            
            if 'error' not in benchmark:
                print(f"   Throughput: {benchmark.get('tags_per_second', 0):.0f} tags/second")
                print(f"   Success Rate: {benchmark.get('success_rate_percent', 0):.1f}%")
                print(f"   Avg Response: {benchmark.get('avg_response_time_ms', 0):.2f}ms")
                print(f"   P95 Response: {benchmark.get('p95_response_time_ms', 0):.2f}ms")
                print(f"   P99 Response: {benchmark.get('p99_response_time_ms', 0):.2f}ms")
            else:
                print(f"   Benchmark Error: {benchmark['error']}")
                
        except Exception as e:
            print(f"   Error in performance benchmark: {e}")
        print()
        
        # Example 7: Batch stress testing
        print("7. Batch Stress Test:")
        try:
            stress_tags = [f'StressTest_{i}' for i in range(25)]
            stress_test = eip_manager.stress_test_batch(
                '192.168.1.100', stress_tags, iterations=50
            )
            
            if 'error' not in stress_test:
                print(f"   Batch Throughput: {stress_test.get('tags_per_second', 0):.0f} tags/second")
                print(f"   Batch Success Rate: {stress_test.get('batch_success_rate_percent', 0):.1f}%")
                print(f"   Tag Success Rate: {stress_test.get('tag_success_rate_percent', 0):.1f}%")
                print(f"   Avg Batch Time: {stress_test.get('avg_batch_time_ms', 0):.2f}ms")
            else:
                print(f"   Stress Test Error: {stress_test['error']}")
                
        except Exception as e:
            print(f"   Error in stress test: {e}")
        print()
        
        # Example 8: Real-time monitoring simulation
        print("8. Real-time Monitoring Simulation (10 seconds):")
        try:
            monitor_start = time.time()
            read_count = 0
            error_count = 0
            
            while time.time() - monitor_start < 10.0:
                try:
                    value = eip_manager.read_tag('192.168.1.100', 'MonitorTag')
                    if value is not None:
                        read_count += 1
                    else:
                        error_count += 1
                except:
                    error_count += 1
                
                time.sleep(0.02)  # 50Hz monitoring rate
            
            monitoring_time = time.time() - monitor_start
            total_attempts = read_count + error_count
            success_rate = (read_count / max(total_attempts, 1)) * 100
            
            print(f"   Monitoring Rate: {read_count/monitoring_time:.0f} successful reads/second")
            print(f"   Success Rate: {success_rate:.1f}% ({read_count}/{total_attempts})")
            print(f"   Total Time: {monitoring_time:.1f}s")
            
        except Exception as e:
            print(f"   Error in real-time monitoring: {e}")
        print()
        
        # Final system status
        print("9. Final System Status:")
        try:
            final_status = eip_manager.get_system_status()
            if 'error' not in final_status:
                sys_stats = final_status['system']
                print(f"   Final Stats - Requests: {sys_stats.get('total_requests', 0)}, "
                      f"Success: {sys_stats.get('success_rate_percent', 0):.1f}%, "
                      f"Rate: {sys_stats.get('requests_per_second', 0):.1f} req/s")
            
        except Exception as e:
            print(f"   Error getting final status: {e}")
        
        print("\n=== Demonstration Complete ===")
        print("System performed comprehensive industrial PLC communication testing")
        print("including single reads, batch operations, multi-PLC coordination,")
        print("UDT handling, performance benchmarking, and real-time monitoring.")
        
    except KeyboardInterrupt:
        print("\nInterrupted by user")
    except Exception as e:
        print(f"Critical system error: {e}")
        traceback.print_exc()
    finally:
        # Always shutdown gracefully
        print("\nShutting down system...")
        eip_manager.shutdown()
        print("System shutdown complete")

# =============================================================================
# Advanced Utilities and Tools
# =============================================================================

class PLCConnectionTester:
    """Utility class for testing PLC connections and diagnosing issues"""
    
    def __init__(self, eip_manager: IndustrialZMQEIPManager):
        self.eip_manager = eip_manager
    
    def test_connection(self, plc_ip: str, test_tag: str = 'TestTag') -> Dict[str, Any]:
        """Comprehensive connection test"""
        results = {
            'plc_ip': plc_ip,
            'timestamp': time.time(),
            'tests': {}
        }
        
        # Test 1: Basic connectivity
        try:
            start_time = time.perf_counter()
            value = self.eip_manager.read_tag(plc_ip, test_tag, timeout=10.0)
            response_time = (time.perf_counter() - start_time) * 1000
            
            results['tests']['basic_read'] = {
                'success': value is not None,
                'value': value,
                'response_time_ms': response_time,
                'status': 'PASS' if value is not None else 'FAIL'
            }
        except Exception as e:
            results['tests']['basic_read'] = {
                'success': False,
                'error': str(e),
                'status': 'ERROR'
            }
        
        # Test 2: Multiple rapid reads
        try:
            rapid_results = []
            for i in range(10):
                start_time = time.perf_counter()
                value = self.eip_manager.read_tag(plc_ip, test_tag, timeout=5.0)
                response_time = (time.perf_counter() - start_time) * 1000
                rapid_results.append({
                    'success': value is not None,
                    'response_time_ms': response_time
                })
            
            success_count = sum(1 for r in rapid_results if r['success'])
            avg_time = sum(r['response_time_ms'] for r in rapid_results if r['success']) / max(success_count, 1)
            
            results['tests']['rapid_reads'] = {
                'success_count': success_count,
                'total_attempts': 10,
                'success_rate': success_count / 10 * 100,
                'avg_response_time_ms': avg_time,
                'status': 'PASS' if success_count >= 8 else 'FAIL'
            }
        except Exception as e:
            results['tests']['rapid_reads'] = {
                'success': False,
                'error': str(e),
                'status': 'ERROR'
            }
        
        # Test 3: Write capability (if safe test tag available)
        try:
            original_value = self.eip_manager.read_tag(plc_ip, test_tag)
            if original_value is not None:
                test_value = 12345
                write_success = self.eip_manager.write_tag(plc_ip, test_tag, test_value)
                
                if write_success:
                    # Verify write
                    verify_value = self.eip_manager.read_tag(plc_ip, test_tag)
                    write_verified = verify_value == test_value
                    
                    # Restore original value
                    self.eip_manager.write_tag(plc_ip, test_tag, original_value)
                else:
                    write_verified = False
                
                results['tests']['write_capability'] = {
                    'write_success': write_success,
                    'write_verified': write_verified,
                    'status': 'PASS' if write_success and write_verified else 'FAIL'
                }
            else:
                results['tests']['write_capability'] = {
                    'status': 'SKIP',
                    'reason': 'Could not read initial value'
                }
        except Exception as e:
            results['tests']['write_capability'] = {
                'success': False,
                'error': str(e),
                'status': 'ERROR'
            }
        
        # Overall status
        test_results = [t.get('status', 'UNKNOWN') for t in results['tests'].values()]
        if 'ERROR' in test_results:
            results['overall_status'] = 'ERROR'
        elif 'FAIL' in test_results:
            results['overall_status'] = 'FAIL'
        else:
            results['overall_status'] = 'PASS'
        
        return results

class PLCDataLogger:
    """High-performance data logging utility"""
    
    def __init__(self, eip_manager: IndustrialZMQEIPManager, log_file: str = None):
        self.eip_manager = eip_manager
        self.log_file = log_file or f"plc_data_{int(time.time())}.csv"
        self.running = False
        self.log_thread = None
        self.data_queue = Queue()
    
    def start_logging(self, plc_configs: List[Dict], interval_sec: float = 1.0):
        """Start high-frequency data logging"""
        self.running = True
        self.log_thread = threading.Thread(
            target=self._log_worker,
            args=(plc_configs, interval_sec),
            daemon=True
        )
        self.log_thread.start()
        logging.info(f"Started data logging to {self.log_file}")
    
    def stop_logging(self):
        """Stop data logging"""
        self.running = False
        if self.log_thread:
            self.log_thread.join(timeout=5)
        logging.info("Data logging stopped")
    
    def _log_worker(self, plc_configs: List[Dict], interval_sec: float):
        """Background worker for data logging"""
        try:
            with open(self.log_file, 'w', newline='') as csvfile:
                # Write header
                header = ['timestamp']
                for config in plc_configs:
                    for tag in config['tags']:
                        header.append(f"{config['plc_ip']}.{tag}")
                
                csvfile.write(','.join(header) + '\n')
                csvfile.flush()
                
                while self.running:
                    try:
                        timestamp = time.time()
                        row_data = [str(timestamp)]
                        
                        # Read all configured tags
                        for config in plc_configs:
                            values = self.eip_manager.read_tags_batch(
                                config['plc_ip'], 
                                config['tags'], 
                                timeout=interval_sec * 0.8
                            )
                            
                            for value in values:
                                row_data.append(str(value) if value is not None else 'NULL')
                        
                        # Write row
                        csvfile.write(','.join(row_data) + '\n')
                        csvfile.flush()
                        
                        # Sleep until next interval
                        time.sleep(interval_sec)
                        
                    except Exception as e:
                        logging.error(f"Error in data logging: {e}")
                        time.sleep(1)  # Brief pause on error
                        
        except Exception as e:
            logging.error(f"Critical error in data logger: {e}")

# =============================================================================
# Main Execution
# =============================================================================

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == '--create-stub':
            create_libeip_stub()
        elif sys.argv[1] == '--test':
            # Quick validation test
            try:
                config = EIPConfig()
                print(f"Configuration validation: {config.validate()}")
                print("Basic imports and classes working correctly")
            except Exception as e:
                print(f"Validation error: {e}")
        elif sys.argv[1] == '--help':
            print("Industrial-Grade ZMQ + libeip PLC Communication System")
            print()
            print("Usage:")
            print("  python industrial_plc_system.py                 # Run full demonstration")
            print("  python industrial_plc_system.py --create-stub   # Create libeip stub for testing")
            print("  python industrial_plc_system.py --test          # Quick validation test")
            print("  python industrial_plc_system.py --help          # Show this help")
            print()
            print("Features:")
            print("  - Industrial-grade error handling and recovery")
            print("  - Memory-safe operations with bounds checking")
            print("  - Circuit breaker pattern for failing PLCs")
            print("  - High-performance batch operations")
            print("  - UDT (User Defined Type) support")
            print("  - Comprehensive monitoring and diagnostics")
            print("  - Production-ready logging and statistics")
        else:
            print(f"Unknown option: {sys.argv[1]}")
            print("Use --help for usage information")
    else:
        main()
    #!/usr/bin/env python3
"""
Production-Grade ZMQ + libeip High-Performance PLC Communication System

This system combines:
- ZMQ for distributed messaging and load balancing
- libeip (C library) for maximum performance EtherNet/IP communication
- Python for easy integration and business logic
- Industrial-grade error handling and memory safety

Performance target: 10,000+ tags/second with <5% CPU usage
Memory safety: Comprehensive bounds checking and validation
Error recovery: Circuit breakers, exponential backoff, comprehensive logging
"""

import zmq
import json
import time
import ctypes
import threading
import multiprocessing
import logging
import os
import struct
import random
import hashlib
from typing import Dict, List, Any, Optional, Union, Tuple
from dataclasses import dataclass, asdict, field
from queue import Queue, Empty
from enum import IntEnum
from collections import defaultdict, deque
import signal
import traceback

# =============================================================================
# Configuration and Constants
# =============================================================================

@dataclass
class EIPConfig:
    """System configuration with validation"""
    # Performance settings
    max_packet_size: int = 4096  # Increased from 1024
    connection_timeout_ms: int = 5000
    heartbeat_interval_sec: int = 30
    worker_count: int = 4
    
    # Reliability settings
    max_retries: int = 5
    initial_backoff_sec: float = 1.0
    max_backoff_sec: float = 60.0
    circuit_breaker_threshold: int = 10
    circuit_breaker_timeout_sec: int = 300
    
    # Memory settings
    max_udt_size: int = 16384  # 16KB max UDT
    max_string_length: int = 1000
    max_array_elements: int = 1000
    
    # Network settings
    zmq_linger_ms: int = 1000
    zmq_high_water_mark: int = 1000
    
    def validate(self) -> bool:
        """Validate configuration parameters"""
        if self.worker_count < 1 or self.worker_count > 32:
            raise ValueError("Worker count must be between 1 and 32")
        if self.max_packet_size < 512 or self.max_packet_size > 65536:
            raise ValueError("Packet size must be between 512 and 65536 bytes")
        if self.connection_timeout_ms < 100 or self.connection_timeout_ms > 60000:
            raise ValueError("Connection timeout must be between 100ms and 60s")
        return True

# =============================================================================
# EtherNet/IP Protocol Definitions
# =============================================================================

class EIPDataType(IntEnum):
    """EtherNet/IP CIP Data Types with size information"""
    BOOL = 0x00C1      # 1 bit (stored as byte)
    SINT = 0x00C6      # 1 byte
    INT = 0x00C7       # 2 bytes
    DINT = 0x00C8      # 4 bytes
    LINT = 0x00C9      # 8 bytes
    USINT = 0x00C2     # 1 byte
    UINT = 0x00C3      # 2 bytes
    UDINT = 0x00C4     # 4 bytes
    ULINT = 0x00C5     # 8 bytes
    REAL = 0x00CA      # 4 bytes
    LREAL = 0x00CB     # 8 bytes
    STRING = 0x00D0    # Variable length
    
    # Array indicators
    ARRAY_BOOL = 0x6001
    ARRAY_SINT = 0x6006
    ARRAY_INT = 0x6007
    ARRAY_DINT = 0x6008
    ARRAY_REAL = 0x600A
    
    # Structure types
    UDT = 0x02A0          # User Defined Type
    STRUCT = 0x02A1       # Generic structure
    
    @classmethod
    def get_size(cls, data_type: int) -> int:
        """Get size in bytes for a data type"""
        size_map = {
            cls.BOOL: 1, cls.SINT: 1, cls.USINT: 1,
            cls.INT: 2, cls.UINT: 2,
            cls.DINT: 4, cls.UDINT: 4, cls.REAL: 4,
            cls.LINT: 8, cls.ULINT: 8, cls.LREAL: 8,
        }
        return size_map.get(data_type, 4)  # Default to 4 bytes

class EIPStatus(IntEnum):
    """EtherNet/IP Status Codes"""
    SUCCESS = 0
    CONNECTION_FAILURE = 1
    INVALID_TAG = 2
    TIMEOUT = 3
    PROTOCOL_ERROR = 4
    MEMORY_ERROR = 5
    VALIDATION_ERROR = 6
    CIRCUIT_BREAKER_OPEN = 7

# =============================================================================
# Memory-Safe C Structures
# =============================================================================

class SafeEIPTagData(ctypes.Structure):
    """Memory-safe tag data structure with bounds checking"""
    MAX_DATA_SIZE = 4096
    
    _fields_ = [
        ("data_type", ctypes.c_uint16),
        ("element_count", ctypes.c_uint16),
        ("data_size", ctypes.c_uint32),
        ("max_data_size", ctypes.c_uint32),  # Added for validation
        ("data", ctypes.c_uint8 * MAX_DATA_SIZE)
    ]
    
    def __init__(self):
        super().__init__()
        self.max_data_size = self.MAX_DATA_SIZE
    
    def validate_size(self, requested_size: int) -> bool:
        """Validate data size before operations"""
        return 0 <= requested_size <= self.MAX_DATA_SIZE
    
    def safe_copy_from(self, source_data: bytes, offset: int = 0) -> bool:
        """Memory-safe data copy with bounds checking"""
        if not isinstance(source_data, (bytes, bytearray)):
            logging.error("Source data must be bytes or bytearray")
            return False
            
        total_size = len(source_data) + offset
        if not self.validate_size(total_size):
            logging.error(f"Buffer overflow prevented: {total_size} > {self.MAX_DATA_SIZE}")
            return False
        
        if offset < 0 or offset >= self.MAX_DATA_SIZE:
            logging.error(f"Invalid offset: {offset}")
            return False
        
        try:
            # Safe copy using ctypes
            ctypes.memmove(
                ctypes.byref(self.data, offset),
                source_data,
                len(source_data)
            )
            self.data_size = total_size
            return True
        except Exception as e:
            logging.error(f"Memory copy failed: {e}")
            return False
    
    def get_data_copy(self) -> bytes:
        """Get a safe copy of the data"""
        if self.data_size > self.MAX_DATA_SIZE:
            logging.error(f"Invalid data size: {self.data_size}")
            return b''
        return bytes(self.data[:self.data_size])

class SafeEIPUDTMember(ctypes.Structure):
    """Memory-safe UDT member definition"""
    _fields_ = [
        ("name", ctypes.c_char * 64),
        ("data_type", ctypes.c_uint16),
        ("offset", ctypes.c_uint16),
        ("array_size", ctypes.c_uint16),
        ("bit_position", ctypes.c_uint8),
        ("reserved", ctypes.c_uint8)
    ]
    
    def validate(self, udt_size: int) -> bool:
        """Validate member definition"""
        if self.offset >= udt_size:
            return False
        if self.array_size == 0 or self.array_size > 1000:  # Sanity check
            return False
        element_size = EIPDataType.get_size(self.data_type)
        total_size = element_size * self.array_size
        return self.offset + total_size <= udt_size

class SafeEIPUDTDefinition(ctypes.Structure):
    """Memory-safe UDT definition"""
    MAX_MEMBERS = 64
    
    _fields_ = [
        ("name", ctypes.c_char * 64),
        ("size", ctypes.c_uint32),
        ("member_count", ctypes.c_uint16),
        ("reserved", ctypes.c_uint16),
        ("members", SafeEIPUDTMember * MAX_MEMBERS)
    ]
    
    def validate(self) -> bool:
        """Validate UDT definition"""
        if self.member_count > self.MAX_MEMBERS:
            return False
        if self.size == 0 or self.size > 16384:  # 16KB max
            return False
        
        # Validate all members
        for i in range(self.member_count):
            if not self.members[i].validate(self.size):
                return False
        
        return True

class EIPSessionInfo(ctypes.Structure):
    """Session information structure"""
    _fields_ = [
        ("session_handle", ctypes.c_uint32),
        ("connection_id", ctypes.c_uint32),
        ("is_connected", ctypes.c_bool),
        ("last_error", ctypes.c_int),
        ("response_time_us", ctypes.c_uint32)
    ]

# =============================================================================
# Error Recovery and Circuit Breaker
# =============================================================================

class CircuitBreaker:
    """Circuit breaker pattern for failing PLCs"""
    
    def __init__(self, failure_threshold: int = 10, timeout_sec: int = 300):
        self.failure_threshold = failure_threshold
        self.timeout_sec = timeout_sec
        self.failure_count = 0
        self.last_failure_time = 0
        self.state = 'CLOSED'  # CLOSED, OPEN, HALF_OPEN
        self._lock = threading.Lock()
    
    def can_execute(self) -> bool:
        """Check if operation can be executed"""
        with self._lock:
            if self.state == 'CLOSED':
                return True
            elif self.state == 'OPEN':
                if time.time() - self.last_failure_time > self.timeout_sec:
                    self.state = 'HALF_OPEN'
                    return True
                return False
            elif self.state == 'HALF_OPEN':
                return True
            return False
    
    def record_success(self):
        """Record successful operation"""
        with self._lock:
            self.failure_count = 0
            self.state = 'CLOSED'
    
    def record_failure(self):
        """Record failed operation"""
        with self._lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            
            if self.failure_count >= self.failure_threshold:
                self.state = 'OPEN'
                logging.warning(f"Circuit breaker opened after {self.failure_count} failures")
            elif self.state == 'HALF_OPEN':
                self.state = 'OPEN'

class EIPErrorRecovery:
    """Comprehensive error recovery system"""
    
    def __init__(self, config: EIPConfig):
        self.config = config
        self.error_counts = defaultdict(int)
        self.circuit_breakers = {}
        self.error_history = defaultdict(lambda: deque(maxlen=100))
        self._lock = threading.Lock()
    
    def get_circuit_breaker(self, plc_ip: str) -> CircuitBreaker:
        """Get or create circuit breaker for PLC"""
        if plc_ip not in self.circuit_breakers:
            self.circuit_breakers[plc_ip] = CircuitBreaker(
                self.config.circuit_breaker_threshold,
                self.config.circuit_breaker_timeout_sec
            )
        return self.circuit_breakers[plc_ip]
    
    def can_attempt_connection(self, plc_ip: str) -> bool:
        """Check if connection attempt is allowed"""
        circuit_breaker = self.get_circuit_breaker(plc_ip)
        return circuit_breaker.can_execute()
    
    def record_success(self, plc_ip: str):
        """Record successful operation"""
        circuit_breaker = self.get_circuit_breaker(plc_ip)
        circuit_breaker.record_success()
        
        with self._lock:
            self.error_counts[plc_ip] = 0
    
    def record_failure(self, plc_ip: str, error_code: int, error_msg: str = ""):
        """Record failed operation"""
        circuit_breaker = self.get_circuit_breaker(plc_ip)
        circuit_breaker.record_failure()
        
        with self._lock:
            self.error_counts[plc_ip] += 1
            self.error_history[plc_ip].append({
                'timestamp': time.time(),
                'error_code': error_code,
                'error_message': error_msg
            })
    
    def get_backoff_delay(self, plc_ip: str) -> float:
        """Calculate exponential backoff delay"""
        error_count = self.error_counts.get(plc_ip, 0)
        if error_count == 0:
            return 0
        
        # Exponential backoff with jitter
        delay = min(
            self.config.initial_backoff_sec * (2 ** min(error_count - 1, 8)),
            self.config.max_backoff_sec
        )
        
        # Add jitter (±20%)
        jitter = random.uniform(0.8, 1.2)
        return delay * jitter

# =============================================================================
# Safe libeip C Library Interface
# =============================================================================

class SafeLibEIPWrapper:
    """Memory-safe Python wrapper for libeip C library"""
    
    def __init__(self, library_path: str = "./libeip.so", config: EIPConfig = None):
        self.config = config or EIPConfig()
        self.config.validate()
        
        try:
            self.lib = ctypes.CDLL(library_path)
            self._setup_function_signatures()
            self._configure_logging()
            logging.info("libeip library loaded successfully")
        except OSError as e:
            logging.error(f"Failed to load libeip library: {e}")
            raise
    
    def _configure_logging(self):
        """Configure libeip logging"""
        log_level = os.getenv('LIBEIP_LOG', 'INFO').upper()
        level_map = {'DEBUG': 0, 'INFO': 1, 'WARNING': 2, 'ERROR': 3, 'CRITICAL': 4}
        libeip_level = level_map.get(log_level, 1)
        
        if hasattr(self.lib, 'eip_set_log_level'):
            self.lib.eip_set_log_level.argtypes = [ctypes.c_int]
            self.lib.eip_set_log_level.restype = None
            self.lib.eip_set_log_level(libeip_level)
            logging.info(f"libeip log level set to {log_level}")
        
        if log_level == 'DEBUG' and hasattr(self.lib, 'eip_enable_debug'):
            self.lib.eip_enable_debug.argtypes = [ctypes.c_bool]
            self.lib.eip_enable_debug.restype = None
            self.lib.eip_enable_debug(True)
    
    def _setup_function_signatures(self):
        """Configure C function signatures for type safety"""
        
        # Session management
        self.lib.eip_session_create.argtypes = [ctypes.c_char_p, ctypes.c_uint16]
        self.lib.eip_session_create.restype = ctypes.c_void_p
        
        self.lib.eip_session_destroy.argtypes = [ctypes.c_void_p]
        self.lib.eip_session_destroy.restype = None
        
        self.lib.eip_session_connect.argtypes = [ctypes.c_void_p]
        self.lib.eip_session_connect.restype = ctypes.c_int
        
        self.lib.eip_session_disconnect.argtypes = [ctypes.c_void_p]
        self.lib.eip_session_disconnect.restype = None
        
        self.lib.eip_session_get_info.argtypes = [ctypes.c_void_p, ctypes.POINTER(EIPSessionInfo)]
        self.lib.eip_session_get_info.restype = ctypes.c_int
        
        # Tag operations with safe structures
        self.lib.eip_read_tag.argtypes = [
            ctypes.c_void_p,
            ctypes.c_char_p,
            ctypes.POINTER(SafeEIPTagData)
        ]
        self.lib.eip_read_tag.restype = ctypes.c_int
        
        self.lib.eip_write_tag.argtypes = [
            ctypes.c_void_p,
            ctypes.c_char_p,
            ctypes.c_uint16,
            ctypes.c_void_p,
            ctypes.c_uint32
        ]
        self.lib.eip_write_tag.restype = ctypes.c_int
        
        # Batch operations
        self.lib.eip_read_tag_list.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(ctypes.c_char_p),
            ctypes.POINTER(SafeEIPTagData),
            ctypes.c_uint32
        ]
        self.lib.eip_read_tag_list.restype = ctypes.c_int
        
        # UDT operations with safe structures
        self.lib.eip_read_udt_definition.argtypes = [
            ctypes.c_void_p,
            ctypes.c_char_p,
            ctypes.POINTER(SafeEIPUDTDefinition)
        ]
        self.lib.eip_read_udt_definition.restype = ctypes.c_int
        
        self.lib.eip_read_udt_tag.argtypes = [
            ctypes.c_void_p,
            ctypes.c_char_p,
            ctypes.c_char_p,
            ctypes.POINTER(SafeEIPTagData)
        ]
        self.lib.eip_read_udt_tag.restype = ctypes.c_int
        
        self.lib.eip_write_udt_tag.argtypes = [
            ctypes.c_void_p,
            ctypes.c_char_p,
            ctypes.c_char_p,
            ctypes.c_void_p,
            ctypes.c_uint32
        ]
        self.lib.eip_write_udt_tag.restype = ctypes.c_int
        
        # Diagnostics
        self.lib.eip_send_heartbeat.argtypes = [ctypes.c_void_p]
        self.lib.eip_send_heartbeat.restype = ctypes.c_int
        
        self.lib.eip_get_connection_stats.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(ctypes.c_uint32),
            ctypes.POINTER(ctypes.c_uint32),
            ctypes.POINTER(ctypes.c_uint32),
            ctypes.POINTER(ctypes.c_uint32)
        ]
        self.lib.eip_get_connection_stats.restype = ctypes.c_int

# =============================================================================
# Message Protocol for ZMQ
# =============================================================================

@dataclass
class EIPRequest:
    """Request message for EIP operations with validation"""
    request_id: str
    plc_ip: str
    plc_port: int = 44818
    operation: str = 'read'
    tag_name: str = ""
    tag_names: List[str] = field(default_factory=list)
    value: Any = None
    data_type: int = EIPDataType.DINT
    udt_name: str = ""
    udt_data: Dict[str, Any] = field(default_factory=dict)
    timeout_ms: int = 5000
    priority: int = 0
    
    def validate(self) -> bool:
        """Validate request data"""
        if not self.request_id or not self.plc_ip:
            return False
        
        if self.operation not in ['read', 'write', 'read_batch', 'read_udt', 'write_udt', 
                                 'get_udt_definition', 'status', 'heartbeat']:
            return False
        
        if self.operation in ['read', 'write'] and not self.tag_name:
            return False
        
        if self.operation == 'read_batch' and not self.tag_names:
            return False
        
        if self.operation in ['read_udt', 'write_udt'] and (not self.tag_name or not self.udt_name):
            return False
        
        if self.plc_port < 1 or self.plc_port > 65535:
            return False
        
        return True

@dataclass
class EIPResponse:
    """Response message from EIP operations"""
    request_id: str
    success: bool
    value: Any = None
    values: List[Any] = field(default_factory=list)
    udt_definition: Dict[str, Any] = field(default_factory=dict)
    udt_data: Dict[str, Any] = field(default_factory=dict)
    error_code: int = 0
    error_message: str = ""
    response_time_us: int = 0
    timestamp: float = field(default_factory=time.time)
    worker_id: int = -1

# =============================================================================
# Safe Data Parsing Utilities
# =============================================================================

class SafeDataParser:
    """Memory-safe data parsing utilities"""
    
    def __init__(self, config: EIPConfig):
        self.config = config
    
    def parse_tag_data(self, tag_data: SafeEIPTagData) -> Any:
        """Parse tag data with comprehensive validation"""
        try:
            if not tag_data.validate_size(tag_data.data_size):
                logging.error(f"Invalid tag data size: {tag_data.data_size}")
                return None
            
            data_bytes = tag_data.get_data_copy()
            if not data_bytes:
                return None
            
            data_type = tag_data.data_type
            element_count = tag_data.element_count
            
            if element_count == 1:
                return self._parse_single_value(data_bytes, data_type)
            else:
                return self._parse_array_values(data_bytes, data_type, element_count)
                
        except Exception as e:
            logging.error(f"Error parsing tag data: {e}")
            return None
    
    def _parse_single_value(self, data_bytes: bytes, data_type: int) -> Any:
        """Parse single value with type validation"""
        try:
            required_size = EIPDataType.get_size(data_type)
            
            if data_type == EIPDataType.STRING:
                return self._parse_string(data_bytes)
            
            if len(data_bytes) < required_size:
                logging.warning(f"Insufficient data for type {data_type}: {len(data_bytes)} < {required_size}")
                return None
            
            if data_type == EIPDataType.BOOL:
                return bool(data_bytes[0])
            elif data_type == EIPDataType.SINT:
                return struct.unpack('<b', data_bytes[:1])[0]
            elif data_type == EIPDataType.USINT:
                return struct.unpack('<B', data_bytes[:1])[0]
            elif data_type == EIPDataType.INT:
                return struct.unpack('<h', data_bytes[:2])[0]
            elif data_type == EIPDataType.UINT:
                return struct.unpack('<H', data_bytes[:2])[0]
            elif data_type == EIPDataType.DINT:
                return struct.unpack('<i', data_bytes[:4])[0]
            elif data_type == EIPDataType.UDINT:
                return struct.unpack('<I', data_bytes[:4])[0]
            elif data_type == EIPDataType.LINT:
                return struct.unpack('<q', data_bytes[:8])[0]
            elif data_type == EIPDataType.ULINT:
                return struct.unpack('<Q', data_bytes[:8])[0]
            elif data_type == EIPDataType.REAL:
                return struct.unpack('<f', data_bytes[:4])[0]
            elif data_type == EIPDataType.LREAL:
                return struct.unpack('<d', data_bytes[:8])[0]
            
            # Unknown type
            logging.warning(f"Unknown data type: {data_type}")
            return data_bytes
            
        except struct.error as e:
            logging.error(f"Struct unpacking error: {e}")
            return None
    
    def _parse_array_values(self, data_bytes: bytes, data_type: int, element_count: int) -> List[Any]:
        """Parse array values with bounds checking"""
        if element_count > self.config.max_array_elements:
            logging.error(f"Array too large: {element_count} > {self.config.max_array_elements}")
            return []
        
        values = []
        element_size = EIPDataType.get_size(data_type)
        
        for i in range(element_count):
            offset = i * element_size
            if offset + element_size > len(data_bytes):
                logging.warning(f"Array element {i} extends beyond data")
                break
            
            element_data = data_bytes[offset:offset + element_size]
            value = self._parse_single_value(element_data, data_type)
            values.append(value)
        
        return values
    
    def _parse_string(self, data_bytes: bytes) -> Optional[str]:
        """Parse string with length validation"""
        if len(data_bytes) < 2:
            return None
        
        try:
            str_len = struct.unpack('<H', data_bytes[:2])[0]
            
            if str_len > self.config.max_string_length:
                logging.warning(f"String too long: {str_len} > {self.config.max_string_length}")
                str_len = self.config.max_string_length
            
            if 2 + str_len > len(data_bytes):
                logging.warning(f"String length exceeds data: {2 + str_len} > {len(data_bytes)}")
                str_len = len(data_bytes) - 2
            
            if str_len <= 0:
                return ""
            
            return data_bytes[2:2 + str_len].decode('ascii', errors='replace')
            
        except struct.error as e:
            logging.error(f"String parsing error: {e}")
            return None
    
    def encode_value(self, value: Any, data_type: int) -> Tuple[Optional[bytes], int]:
        """Encode Python value for writing with validation"""
        try:
            if data_type == EIPDataType.BOOL:
                data = struct.pack('<B', 1 if value else 0)
            elif data_type == EIPDataType.SINT:
                if not (-128 <= int(value) <= 127):
                    raise ValueError(f"SINT value out of range: {value}")
                data = struct.pack('<b', int(value))
            elif data_type == EIPDataType.USINT:
                if not (0 <= int(value) <= 255):
                    raise ValueError(f"USINT value out of range: {value}")
                data = struct.pack('<B', int(value))
            elif data_type == EIPDataType.INT:
                if not (-32768 <= int(value) <= 32767):
                    raise ValueError(f"INT value out of range: {value}")
                data = struct.pack('<h', int(value))
            elif data_type == EIPDataType.UINT:
                if not (0 <= int(value) <= 65535):
                    raise ValueError(f"UINT value out of range: {value}")
                data = struct.pack('<H', int(value))
            elif data_type == EIPDataType.DINT:
                if not (-2147483648 <= int(value) <= 2147483647):
                    raise ValueError(f"DINT value out of range: {value}")
                data = struct.pack('<i', int(value))
            elif data_type == EIPDataType.UDINT:
                if not (0 <= int(value) <= 4294967295):
                    raise ValueError(f"UDINT value out of range: {value}")
                data = struct.pack('<I', int(value))
            elif data_type == EIPDataType.LINT:
                data = struct.pack('<q', int(value))
            elif data_type == EIPDataType.ULINT:
                data = struct.pack('<Q', int(value))
            elif data_type == EIPDataType.REAL:
                data = struct.pack('<f', float(value))
            elif data_type == EIPDataType.LREAL:
                data = struct.pack('<d', float(value))
            elif data_type == EIPDataType.STRING:
                s = str(value).encode('ascii', errors='replace')
                if len(s) > self.config.max_string_length:
                    s = s[:self.config.max_string_length]
                data = struct.pack('<H', len(s)) + s
            else:
                raise ValueError(f"Unsupported data type: {data_type}")
            
            return data, len(data)
            
        except Exception as e:
            logging.error(f"Failed to encode value {value} as type {data_type}: {e}")
            return None, 0

# =============================================================================
# Industrial-Grade Native EIP Worker
# =============================================================================

class IndustrialEIPWorker(multiprocessing.Process):
    """Production-grade worker with comprehensive error handling"""
    
    def __init__(self, worker_id: int, ventilator_port: int, sink_port: int, 
                 config: EIPConfig, libeip_path: str = "./libeip.so"):
        super().__init__()
        self.worker_id = worker_id
        self.ventilator_port = ventilator_port
        self.sink_port = sink_port
        self.config = config
        self.libeip_path = libeip_path
        
        # Connection management
        self.sessions = {}  # PLC IP -> session handle
        self.session_metadata = {}  # PLC IP -> connection metadata
        self.last_heartbeat_time = {}  # PLC IP -> last heartbeat timestamp
        
        # Caching and performance
        self.udt_definitions = {}  # UDT name -> definition cache
        self.error_recovery = None  # Will be initialized in run()
        self.data_parser = None
        
        # Statistics
        self.stats = {
            'requests_processed': 0,
            'successful_operations': 0,
            'failed_operations': 0,
            'total_response_time_us': 0,
            'start_time': 0,
            'reconnect_attempts': 0,
            'successful_reconnects': 0,
            'udt_definitions_cached': 0,
            'heartbeats_sent': 0,
            'circuit_breaker_trips': 0
        }
        
        # Process control
        self.running = False
        self.shutdown_event = threading.Event()
        
    def run(self):
        """Main worker process loop with comprehensive error handling"""
        # Setup logging for this process
        logging.basicConfig(
            level=logging.INFO,
            format=f'[Worker-{self.worker_id}] %(asctime)s - %(levelname)s - %(message)s'
        )
        logging.info(f"Industrial EIP Worker {self.worker_id} starting")
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
        
        try:
            # Initialize components
            self.eip = SafeLibEIPWrapper(self.libeip_path, self.config)
            self.error_recovery = EIPErrorRecovery(self.config)
            self.data_parser = SafeDataParser(self.config)
            
            # Setup ZMQ with proper error handling
            context = zmq.Context()
            context.setsockopt(zmq.LINGER, self.config.zmq_linger_ms)
            
            # Receive requests
            receiver = context.socket(zmq.PULL)
            receiver.setsockopt(zmq.RCVHWM, self.config.zmq_high_water_mark)
            receiver.connect(f"tcp://localhost:{self.ventilator_port}")
            
            # Send responses  
            sender = context.socket(zmq.PUSH)
            sender.setsockopt(zmq.SNDHWM, self.config.zmq_high_water_mark)
            sender.connect(f"tcp://localhost:{self.sink_port}")
            
            self.running = True
            self.stats['start_time'] = time.time()
            
            logging.info(f"Worker {self.worker_id} initialized successfully")
            
            # Main processing loop
            while self.running and not self.shutdown_event.is_set():
                try:
                    # Check for requests with timeout
                    if receiver.poll(timeout=1000):  # 1 second timeout
                        message = receiver.recv_json(zmq.NOBLOCK)
                        
                        # Validate and process request
                        if self._validate_message(message):
                            request = EIPRequest(**message)
                            if request.validate():
                                response = self._process_request_safe(request)
                                response.worker_id = self.worker_id
                                
                                # Send response
                                sender.send_json(asdict(response))
                                
                                # Update statistics
                                self._update_stats(response)
                            else:
                                logging.error(f"Invalid request: {message}")
                        else:
                            logging.error(f"Invalid message format: {message}")
                    
                    # Periodic maintenance
                    self._periodic_maintenance()
                    
                except zmq.Again:
                    continue  # No message available
                except Exception as e:
                    logging.error(f"Error in main loop: {e}")
                    traceback.print_exc()
                    
        except Exception as e:
            logging.error(f"Worker {self.worker_id} fatal error: {e}")
            traceback.print_exc()
        finally:
            self._cleanup()
            try:
                context.term()
            except:
                pass
            logging.info(f"Worker {self.worker_id} shut down")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logging.info(f"Worker {self.worker_id} received signal {signum}, shutting down...")
        self.running = False
        self.shutdown_event.set()
    
    def _validate_message(self, message: Dict) -> bool:
        """Validate incoming message structure"""
        if not isinstance(message, dict):
            return False
        
        required_fields = ['request_id', 'plc_ip', 'operation']
        return all(field in message for field in required_fields)
    
    def _process_request_safe(self, request: EIPRequest) -> EIPResponse:
        """Process request with comprehensive error handling"""
        start_time = time.perf_counter()
        
        try:
            # Check circuit breaker
            if not self.error_recovery.can_attempt_connection(request.plc_ip):
                return EIPResponse(
                    request_id=request.request_id,
                    success=False,
                    error_code=EIPStatus.CIRCUIT_BREAKER_OPEN,
                    error_message=f"Circuit breaker open for {request.plc_ip}",
                    timestamp=time.time()
                )
            
            # Get or create session
            session = self._get_session_safe(request.plc_ip, request.plc_port)
            if not session:
                self.error_recovery.record_failure(request.plc_ip, EIPStatus.CONNECTION_FAILURE)
                return EIPResponse(
                    request_id=request.request_id,
                    success=False,
                    error_code=EIPStatus.CONNECTION_FAILURE,
                    error_message=f"Failed to connect to {request.plc_ip}",
                    timestamp=time.time()
                )
            
            # Route to appropriate handler
            handler_map = {
                'read': self._handle_read,
                'write': self._handle_write,
                'read_batch': self._handle_read_batch,
                'read_udt': self._handle_read_udt,
                'write_udt': self._handle_write_udt,
                'get_udt_definition': self._handle_get_udt_definition,
                'status': self._handle_status,
                'heartbeat': self._handle_heartbeat
            }
            
            handler = handler_map.get(request.operation)
            if not handler:
                return EIPResponse(
                    request_id=request.request_id,
                    success=False,
                    error_code=EIPStatus.PROTOCOL_ERROR,
                    error_message=f"Unknown operation: {request.operation}",
                    timestamp=time.time()
                )
            
            # Execute operation
            response = handler(request, session, start_time)
            
            # Record success/failure for error recovery
            if response.success:
                self.error_recovery.record_success(request.plc_ip)
            else:
                self.error_recovery.record_failure(request.plc_ip, response.error_code, response.error_message)
            
            return response
            
        except Exception as e:
            logging.error(f"Unexpected error processing request {request.request_id}: {e}")
            traceback.print_exc()
            
            self.error_recovery.record_failure(request.plc_ip, EIPStatus.PROTOCOL_ERROR, str(e))
            
            return EIPResponse(
                request_id=request.request_id,
                success=False,
                error_code=EIPStatus.PROTOCOL_ERROR,
                error_message=f"Internal error: {str(e)[:100]}",
                response_time_us=int((time.perf_counter() - start_time) * 1_000_000),
                timestamp=time.time()
            )
    
    def _get_session_safe(self, plc_ip: str, plc_port: int):
        """Get or create EIP session with comprehensive error handling"""
        session_key = f"{plc_ip}:{plc_port}"
        
        # Check existing session
        if session_key in self.sessions:
            session = self.sessions[session_key]
            
            # Verify session health
            if self._verify_session_health(session):
                return session
            else:
                logging.warning(f"Session {session_key} unhealthy, removing")
                self._cleanup_session(session_key)
        
        # Create new session with backoff
        return self._create_session_with_backoff(session_key, plc_ip, plc_port)
    
    def _verify_session_health(self, session) -> bool:
        """Verify session is still healthy"""
        try:
            session_info = EIPSessionInfo()
            result = self.eip.lib.eip_session_get_info(session, ctypes.byref(session_info))
            return result == EIPStatus.SUCCESS and session_info.is_connected
        except:
            return False
    
    def _create_session_with_backoff(self, session_key: str, plc_ip: str, plc_port: int):
        """Create session with exponential backoff and jitter"""
        # Check if we need to wait due to backoff
        backoff_delay = self.error_recovery.get_backoff_delay(plc_ip)
        if backoff_delay > 0:
            if session_key not in self.session_metadata:
                self.session_metadata[session_key] = {'last_attempt': 0}
            
            metadata = self.session_metadata[session_key]
            time_since_last = time.time() - metadata.get('last_attempt', 0)
            
            if time_since_last < backoff_delay:
                logging.debug(f"Backoff active for {session_key}, waiting {backoff_delay - time_since_last:.1f}s")
                return None
        
        # Attempt connection
        if session_key not in self.session_metadata:
            self.session_metadata[session_key] = {}
        
        metadata = self.session_metadata[session_key]
        metadata['last_attempt'] = time.time()
        self.stats['reconnect_attempts'] += 1
        
        logging.info(f"Attempting connection to {session_key}")
        
        try:
            # Create session
            session = self.eip.lib.eip_session_create(
                plc_ip.encode('ascii'),
                plc_port
            )
            
            if not session:
                raise Exception("Failed to create session handle")
            
            # Connect with timeout
            result = self.eip.lib.eip_session_connect(session)
            
            if result == EIPStatus.SUCCESS:
                # Success - store session
                self.sessions[session_key] = session
                self.stats['successful_reconnects'] += 1
                logging.info(f"Successfully connected to {session_key}")
                return session
            else:
                # Connection failed
                self.eip.lib.eip_session_destroy(session)
                raise Exception(f"Connection failed with status {result}")
                
        except Exception as e:
            logging.error(f"Failed to connect to {session_key}: {e}")
            return None
    
    def _cleanup_session(self, session_key: str):
        """Safely cleanup a session"""
        if session_key in self.sessions:
            session = self.sessions[session_key]
            try:
                self.eip.lib.eip_session_disconnect(session)
                self.eip.lib.eip_session_destroy(session)
            except:
                pass  # Best effort cleanup
            
            del self.sessions[session_key]
            self.last_heartbeat_time.pop(session_key, None)
            logging.debug(f"Cleaned up session {session_key}")
    
    def _handle_read(self, request: EIPRequest, session, start_time: float) -> EIPResponse:
        """Handle single tag read with validation"""
        try:
            tag_data = SafeEIPTagData()
            
            result = self.eip.lib.eip_read_tag(
                session,
                request.tag_name.encode('ascii'),
                ctypes.byref(tag_data)
            )
            
            response_time_us = int((time.perf_counter() - start_time) * 1_000_000)
            
            if result == EIPStatus.SUCCESS:
                value = self.data_parser.parse_tag_data(tag_data)
                return EIPResponse(
                    request_id=request.request_id,
                    success=True,
                    value=value,
                    response_time_us=response_time_us,
                    timestamp=time.time()
                )
            else:
                return EIPResponse(
                    request_id=request.request_id,
                    success=False,
                    error_code=result,
                    error_message=f"Read failed for {request.tag_name}",
                    response_time_us=response_time_us,
                    timestamp=time.time()
                )
                
        except Exception as e:
            logging.error(f"Exception in read handler: {e}")
            return EIPResponse(
                request_id=request.request_id,
                success=False,
                error_code=EIPStatus.PROTOCOL_ERROR,
                error_message=f"Read exception: {str(e)[:100]}",
                response_time_us=int((time.perf_counter() - start_time) * 1_000_000),
                timestamp=time.time()
            )
    
    def _handle_write(self, request: EIPRequest, session, start_time: float) -> EIPResponse:
        """Handle single tag write with validation"""
        try:
            # Validate and encode value
            data_buffer, data_size = self.data_parser.encode_value(request.value, request.data_type)
            
            if data_buffer is None:
                return EIPResponse(
                    request_id=request.request_id,
                    success=False,
                    error_code=EIPStatus.VALIDATION_ERROR,
                    error_message=f"Invalid value or data type for {request.tag_name}",
                    timestamp=time.time()
                )
            
            result = self.eip.lib.eip_write_tag(
                session,
                request.tag_name.encode('ascii'),
                request.data_type,
                data_buffer,
                data_size
            )
            
            response_time_us = int((time.perf_counter() - start_time) * 1_000_000)
            
            return EIPResponse(
                request_id=request.request_id,
                success=(result == EIPStatus.SUCCESS),
                error_code=result,
                error_message="" if result == EIPStatus.SUCCESS else f"Write failed for {request.tag_name}",
                response_time_us=response_time_us,
                timestamp=time.time()
            )
            
        except Exception as e:
            logging.error(f"Exception in write handler: {e}")
            return EIPResponse(
                request_id=request.request_id,
                success=False,
                error_code=EIPStatus.PROTOCOL_ERROR,
                error_message=f"Write exception: {str(e)[:100]}",
                response_time_us=int((time.perf_counter() - start_time) * 1_000_000),
                timestamp=time.time()
            )
    
    def _handle_read_batch(self, request: EIPRequest, session, start_time: float) -> EIPResponse:
        """Handle batch read with comprehensive validation"""
        try:
            if not request.tag_names or len(request.tag_names) == 0:
                return EIPResponse(
                    request_id=request.request_id,
                    success=False,
                    error_code=EIPStatus.VALIDATION_ERROR,
                    error_message="No tag names provided for batch read",
                    timestamp=time.time()
                )
            
            # Limit batch size for safety
            max_batch_size = 100  # Configurable limit
            if len(request.tag_names) > max_batch_size:
                logging.warning(f"Batch size {len(request.tag_names)} exceeds limit {max_batch_size}, truncating")
                tag_names = request.tag_names[:max_batch_size]
            else:
                tag_names = request.tag_names
            
            # Prepare C arrays
            tag_count = len(tag_names)
            tag_names_array = (ctypes.c_char_p * tag_count)()
            tag_data_array = (SafeEIPTagData * tag_count)()
            
            # Initialize tag data structures
            for i in range(tag_count):
                tag_data_array[i] = SafeEIPTagData()
            
            # Fill tag names with validation
            for i, tag_name in enumerate(tag_names):
                if not isinstance(tag_name, str) or len(tag_name) > 64:
                    return EIPResponse(
                        request_id=request.request_id,
                        success=False,
                        error_code=EIPStatus.VALIDATION_ERROR,
                        error_message=f"Invalid tag name at index {i}",
                        timestamp=time.time()
                    )
                tag_names_array[i] = tag_name.encode('ascii')
            
            # Execute batch read
            result = self.eip.lib.eip_read_tag_list(
                session,
                tag_names_array,
                tag_data_array,
                tag_count
            )
            
            response_time_us = int((time.perf_counter() - start_time) * 1_000_000)
            
            if result == EIPStatus.SUCCESS:
                # Parse all results
                values = []
                for i in range(tag_count):
                    try:
                        value = self.data_parser.parse_tag_data(tag_data_array[i])
                        values.append(value)
                    except Exception as e:
                        logging.error(f"Failed to parse batch result {i}: {e}")
                        values.append(None)
                
                return EIPResponse(
                    request_id=request.request_id,
                    success=True,
                    values=values,
                    response_time_us=response_time_us,
                    timestamp=time.time()
                )
            else:
                return EIPResponse(
                    request_id=request.request_id,
                    success=False,
                    error_code=result,
                    error_message=f"Batch read failed for {tag_count} tags",
                    response_time_us=response_time_us,
                    timestamp=time.time()
                )
                
        except Exception as e:
            logging.error(f"Exception in batch read handler: {e}")
            return EIPResponse(
                request_id=request.request_id,
                success=False,
                error_code=EIPStatus.PROTOCOL_ERROR,
                error_message=f"Batch read exception: {str(e)[:100]}",
                response_time_us=int((time.perf_counter() - start_time) * 1_000_000),
                timestamp=time.time()
            )
    
    def _handle_read_udt(self, request: EIPRequest, session, start_time: float) -> EIPResponse:
        """Handle UDT tag read with validation"""
        try:
            if not request.udt_name or not request.tag_name:
                return EIPResponse(
                    request_id=request.request_id,
                    success=False,
                    error_code=EIPStatus.VALIDATION_ERROR,
                    error_message="UDT name and tag name required for UDT read",
                    timestamp=time.time()
                )
            
            # Get UDT definition (cached)
            udt_def = self._get_udt_definition_safe(session, request.udt_name)
            if not udt_def:
                return EIPResponse(
                    request_id=request.request_id,
                    success=False,
                    error_code=EIPStatus.INVALID_TAG,
                    error_message=f"Failed to get UDT definition for {request.udt_name}",
                    timestamp=time.time()
                )
            
            # Read UDT tag
            tag_data = SafeEIPTagData()
            result = self.eip.lib.eip_read_udt_tag(
                session,
                request.tag_name.encode('ascii'),
                request.udt_name.encode('ascii'),
                ctypes.byref(tag_data)
            )
            
            response_time_us = int((time.perf_counter() - start_time) * 1_000_000)
            
            if result == EIPStatus.SUCCESS:
                # Parse UDT data
                udt_data = self._parse_udt_data_safe(tag_data, udt_def)
                return EIPResponse(
                    request_id=request.request_id,
                    success=True,
                    udt_data=udt_data,
                    response_time_us=response_time_us,
                    timestamp=time.time()
                )
            else:
                return EIPResponse(
                    request_id=request.request_id,
                    success=False,
                    error_code=result,
                    error_message=f"UDT read failed for {request.tag_name}",
                    response_time_us=response_time_us,
                    timestamp=time.time()
                )
                
        except Exception as e:
            logging.error(f"Exception in UDT read handler: {e}")
            return EIPResponse(
                request_id=request.request_id,
                success=False,
                error_code=EIPStatus.PROTOCOL_ERROR,
                error_message=f"UDT read exception: {str(e)[:100]}",
                response_time_us=int((time.perf_counter() - start_time) * 1_000_000),
                timestamp=time.time()
            )
    
    def _handle_write_udt(self, request: EIPRequest, session, start_time: float) -> EIPResponse:
        """Handle UDT tag write with validation"""
        try:
            if not request.udt_name or not request.tag_name or not request.udt_data:
                return EIPResponse(
                    request_id=request.request_id,
                    success=False,
                    error_code=EIPStatus.VALIDATION_ERROR,
                    error_message="UDT name, tag name, and data required for UDT write",
                    timestamp=time.time()
                )
            
            # Get UDT definition
            udt_def = self._get_udt_definition_safe(session, request.udt_name)
            if not udt_def:
                return EIPResponse(
                    request_id=request.request_id,
                    success=False,
                    error_code=EIPStatus.INVALID_TAG,
                    error_message=f"Failed to get UDT definition for {request.udt_name}",
                    timestamp=time.time()
                )
            
            # Encode UDT data
            data_buffer, data_size = self._encode_udt_data_safe(request.udt_data, udt_def)
            if data_buffer is None:
                return EIPResponse(
                    request_id=request.request_id,
                    success=False,
                    error_code=EIPStatus.VALIDATION_ERROR,
                    error_message="Failed to encode UDT data",
                    timestamp=time.time()
                )
            
            # Write UDT tag
            result = self.eip.lib.eip_write_udt_tag(
                session,
                request.tag_name.encode('ascii'),
                request.udt_name.encode('ascii'),
                data_buffer,
                data_size
            )
            
            response_time_us = int((time.perf_counter() - start_time) * 1_000_000)
            
            return EIPResponse(
                request_id=request.request_id,
                success=(result == EIPStatus.SUCCESS),
                error_code=result,
                error_message="" if result == EIPStatus.SUCCESS else f"UDT write failed for {request.tag_name}",
                response_time_us=response_time_us,
                timestamp=time.time()
            )
            
        except Exception as e:
            logging.error(f"Exception in UDT write handler: {e}")
            return EIPResponse(
                request_id=request.request_id,
                success=False,
                error_code=EIPStatus.PROTOCOL_ERROR,
                error_message=f"UDT write exception: {str(e)[:100]}",
                response_time_us=int((time.perf_counter() - start_time) * 1_000_000),
                timestamp=time.time()
            )
    
    def _handle_get_udt_definition(self, request: EIPRequest, session, start_time: float) -> EIPResponse:
        """Handle UDT definition retrieval"""
        try:
            if not request.udt_name:
                return EIPResponse(
                    request_id=request.request_id,
                    success=False,
                    error_code=EIPStatus.VALIDATION_ERROR,
                    error_message="UDT name required",
                    timestamp=time.time()
                )
            
            udt_def = self._get_udt_definition_safe(session, request.udt_name)
            response_time_us = int((time.perf_counter() - start_time) * 1_000_000)
            
            if udt_def:
                return EIPResponse(
                    request_id=request.request_id,
                    success=True,
                    udt_definition=udt_def,
                    response_time_us=response_time_us,
                    timestamp=time.time()
                )
            else:
                return EIPResponse(
                    request_id=request.request_id,
                    success=False,
                    error_code=EIPStatus.INVALID_TAG,
                    error_message=f"Failed to get UDT definition for {request.udt_name}",
                    response_time_us=response_time_us,
                    timestamp=time.time()
                )
                
        except Exception as e:
            logging.error(f"Exception in UDT definition handler: {e}")
            return EIPResponse(
                request_id=request.request_id,
                success=False,
                error_code=EIPStatus.PROTOCOL_ERROR,
                error_message=f"UDT definition exception: {str(e)[:100]}",
                response_time_us=int((time.perf_counter() - start_time) * 1_000_000),
                timestamp=time.time()
            )
    
    def _handle_status(self, request: EIPRequest, session, start_time: float) -> EIPResponse:
        """Handle status/diagnostics request"""
        try:
            session_info = EIPSessionInfo()
            result = self.eip.lib.eip_session_get_info(session, ctypes.byref(session_info))
            
            status_data = {
                'plc_ip': request.plc_ip,
                'worker_id': self.worker_id,
                'session_connected': result == EIPStatus.SUCCESS and session_info.is_connected,
                'worker_stats': self.stats.copy(),
                'error_recovery_stats': {
                    'circuit_breakers': {ip: cb.state for ip, cb in self.error_recovery.circuit_breakers.items()},
                    'error_counts': dict(self.error_recovery.error_counts),
                },
                'active_sessions': len(self.sessions),
                'cached_udts': len(self.udt_definitions)
            }
            
            if result == EIPStatus.SUCCESS:
                # Get connection statistics
                packets_sent = ctypes.c_uint32()
                packets_received = ctypes.c_uint32()
                errors = ctypes.c_uint32()
                avg_response_time = ctypes.c_uint32()
                
                stats_result = self.eip.lib.eip_get_connection_stats(
                    session,
                    ctypes.byref(packets_sent),
                    ctypes.byref(packets_received),
                    ctypes.byref(errors),
                    ctypes.byref(avg_response_time)
                )
                
                if stats_result == EIPStatus.SUCCESS:
                    status_data.update({
                        'session_handle': session_info.session_handle,
                        'connection_id': session_info.connection_id,
                        'packets_sent': packets_sent.value,
                        'packets_received': packets_received.value,
                        'connection_errors': errors.value,
                        'avg_response_time_us': avg_response_time.value,
                    })
            
            return EIPResponse(
                request_id=request.request_id,
                success=True,
                value=status_data,
                response_time_us=int((time.perf_counter() - start_time) * 1_000_000),
                timestamp=time.time()
            )
            
        except Exception as e:
            logging.error(f"Exception in status handler: {e}")
            return EIPResponse(
                request_id=request.request_id,
                success=False,
                error_code=EIPStatus.PROTOCOL_ERROR,
                error_message=f"Status exception: {str(e)[:100]}",
                response_time_us=int((time.perf_counter() - start_time) * 1_000_000),
                timestamp=time.time()
            )
    
    def _handle_heartbeat(self, request: EIPRequest, session, start_time: float) -> EIPResponse:
        """Handle manual heartbeat request"""
        try:
            result = self.eip.lib.eip_send_heartbeat(session)
            response_time_us = int((time.perf_counter() - start_time) * 1_000_000)
            
            if result == EIPStatus.SUCCESS:
                self.stats['heartbeats_sent'] += 1
                session_key = f"{request.plc_ip}:{request.plc_port}"
                self.last_heartbeat_time[session_key] = time.time()
            
            return EIPResponse(
                request_id=request.request_id,
                success=(result == EIPStatus.SUCCESS),
                error_code=result,
                response_time_us=response_time_us,
                timestamp=time.time()
            )
            
        except Exception as e:
            logging.error(f"Exception in heartbeat handler: {e}")
            return EIPResponse(
                request_id=request.request_id,
                success=False,
                error_code=EIPStatus.PROTOCOL_ERROR,
                error_message=f"Heartbeat exception: {str(e)[:100]}",
                response_time_us=int((time.perf_counter() - start_time) * 1_000_000),
                timestamp=time.time()
            )
    
    def _get_udt_definition_safe(self, session, udt_name: str) -> Optional[Dict[str, Any]]:
        """Get UDT definition with caching and validation"""
        try:
            # Check cache first
            if udt_name in self.udt_definitions:
                return self.udt_definitions[udt_name]
            
            # Validate UDT name
            if not udt_name or len(udt_name) > 63:
                logging.error(f"Invalid UDT name: {udt_name}")
                return None
            
            # Fetch from PLC
            udt_def = SafeEIPUDTDefinition()
            result = self.eip.lib.eip_read_udt_definition(
                session,
                udt_name.encode('ascii'),
                ctypes.byref(udt_def)
            )
            
            if result != EIPStatus.SUCCESS:
                logging.error(f"Failed to read UDT definition for {udt_name}: {result}")
                return None
            
            # Validate UDT definition
            if not udt_def.validate():
                logging.error(f"Invalid UDT definition structure for {udt_name}")
                return None
            
            # Convert to Python dict
            definition = {
                'name': udt_def.name.decode('ascii', errors='replace'),
                'size': udt_def.size,
                'member_count': udt_def.member_count,
                'members': []
            }
            
            # Extract member definitions with validation
            for i in range(min(udt_def.member_count, SafeEIPUDTDefinition.MAX_MEMBERS)):
                member = udt_def.members[i]
                
                member_def = {
                    'name': member.name.decode('ascii', errors='replace'),
                    'data_type': member.data_type,
                    'offset': member.offset,
                    'array_size': member.array_size,
                    'bit_position': member.bit_position if member.data_type == EIPDataType.BOOL else None
                }
                
                # Validate member
                if member_def['name'] and member_def['offset'] < udt_def.size:
                    definition['members'].append(member_def)
                else:
                    logging.warning(f"Skipping invalid UDT member at index {i}")
            
            # Cache definition
            self.udt_definitions[udt_name] = definition
            self.stats['udt_definitions_cached'] += 1
            
            logging.debug(f"Cached UDT definition for {udt_name} with {len(definition['members'])} members")
            return definition
            
        except Exception as e:
            logging.error(f"Exception getting UDT definition for {udt_name}: {e}")
            return None
    
    def _parse_udt_data_safe(self, tag_data: SafeEIPTagData, udt_def: Dict[str, Any]) -> Dict[str, Any]:
        """Parse UDT data with comprehensive validation"""
        if not tag_data.validate_size(tag_data.data_size):
            logging.error(f"Invalid UDT data size: {tag_data.data_size}")
            return {}
        
        data_bytes = tag_data.get_data_copy()
        if not data_bytes:
            logging.error("Failed to get UDT data copy")
            return {}
        
        udt_data = {}
        
        try:
            # Validate UDT definition
            if not self._validate_udt_definition(udt_def):
                logging.error("Invalid UDT definition for parsing")
                return {}
            
            for member in udt_def.get('members', []):
                try:
                    name = member.get('name', '')
                    if not name:
                        continue
                    
                    data_type = member.get('data_type', 0)
                    offset = member.get('offset', 0)
                    array_size = member.get('array_size', 1)
                    bit_position = member.get('bit_position')
                    
                    # Bounds checking
                    if offset < 0 or offset >= len(data_bytes):
                        logging.warning(f"UDT member {name} offset {offset} out of bounds")
                        udt_data[name] = None
                        continue
                    
                    # Calculate total size needed
                    element_size = EIPDataType.get_size(data_type)
                    total_size = element_size * array_size
                    
                    if offset + total_size > len(data_bytes):
                        logging.warning(f"UDT member {name} extends beyond data bounds")
                        udt_data[name] = None
                        continue
                    
                    # Parse based on array size
                    if array_size == 1:
                        # Scalar member
                        value = self._parse_udt_member_safe(data_bytes, offset, data_type, bit_position)
                        udt_data[name] = value
                    else:
                        # Array member
                        values = []
                        for i in range(min(array_size, self.config.max_array_elements)):
                            element_offset = offset + (i * element_size)
                            if element_offset + element_size <= len(data_bytes):
                                value = self._parse_udt_member_safe(data_bytes, element_offset, data_type)
                                values.append(value)
                            else:
                                values.append(None)
                        udt_data[name] = values
                        
                except Exception as e:
                    logging.error(f"Failed to parse UDT member {member.get('name', 'unknown')}: {e}")
                    udt_data[member.get('name', f'member_{len(udt_data)}')] = None
            
            return udt_data
            
        except Exception as e:
            logging.error(f"Critical error parsing UDT data: {e}")
            return {}
    
    def _parse_udt_member_safe(self, data_bytes: bytes, offset: int, data_type: int, 
                              bit_position: Optional[int] = None) -> Any:
        """Parse individual UDT member with comprehensive validation"""
        try:
            required_size = EIPDataType.get_size(data_type)
            
            # Bounds check
            if offset < 0 or offset + required_size > len(data_bytes):
                logging.warning(f"UDT member parse bounds check failed: offset={offset}, size={required_size}")
                return None
            
            if data_type == EIPDataType.BOOL:
                if bit_position is not None and required_size >= 4:
                    # BOOL packed in DWORD
                    if offset + 4 <= len(data_bytes):
                        dword = struct.unpack('<I', data_bytes[offset:offset+4])[0]
                        return bool(dword & (1 << min(bit_position, 31)))
                    return None
                else:
                    # Standalone BOOL
                    return bool(data_bytes[offset])
            elif data_type == EIPDataType.SINT:
                return struct.unpack('<b', data_bytes[offset:offset+1])[0]
            elif data_type == EIPDataType.USINT:
                return struct.unpack('<B', data_bytes[offset:offset+1])[0]
            elif data_type == EIPDataType.INT:
                return struct.unpack('<h', data_bytes[offset:offset+2])[0]
            elif data_type == EIPDataType.UINT:
                return struct.unpack('<H', data_bytes[offset:offset+2])[0]
            elif data_type == EIPDataType.DINT:
                return struct.unpack('<i', data_bytes[offset:offset+4])[0]
            elif data_type == EIPDataType.UDINT:
                return struct.unpack('<I', data_bytes[offset:offset+4])[0]
            elif data_type == EIPDataType.LINT:
                return struct.unpack('<q', data_bytes[offset:offset+8])[0]
            elif data_type == EIPDataType.ULINT:
                return struct.unpack('<Q', data_bytes[offset:offset+8])[0]
            elif data_type == EIPDataType.REAL:
                return struct.unpack('<f', data_bytes[offset:offset+4])[0]
            elif data_type == EIPDataType.LREAL:
                return struct.unpack('<d', data_bytes[offset:offset+8])[0]
            elif data_type == EIPDataType.STRING:
                return self._parse_udt_string_safe(data_bytes[offset:])
            
            # Unknown type
            logging.warning(f"Unknown UDT member data type: {data_type}")
            return None
            
        except struct.error as e:
            logging.error(f"Struct unpacking error at offset {offset}: {e}")
            return None
        except Exception as e:
            logging.error(f"Unexpected error parsing UDT member: {e}")
            return None
    
    def _parse_udt_string_safe(self, data_bytes: bytes) -> Optional[str]:
        """Parse UDT string member safely"""
        try:
            if len(data_bytes) < 2:
                return None
            
            str_len = struct.unpack('<H', data_bytes[:2])[0]
            
            # Validate string length
            if str_len > self.config.max_string_length:
                logging.warning(f"UDT string too long: {str_len} > {self.config.max_string_length}")
                str_len = self.config.max_string_length
            
            if 2 + str_len > len(data_bytes):
                logging.warning(f"UDT string extends beyond data")
                str_len = max(0, len(data_bytes) - 2)
            
            if str_len <= 0:
                return ""
            
            return data_bytes[2:2 + str_len].decode('ascii', errors='replace')
            
        except Exception as e:
            logging.error(f"Error parsing UDT string: {e}")
            return None
    
    def _encode_udt_data_safe(self, udt_data: Dict[str, Any], udt_def: Dict[str, Any]) -> Tuple[Optional[ctypes.c_char_p], int]:
        """Encode UDT data for writing with comprehensive validation"""
        try:
            # Validate inputs
            if not isinstance(udt_data, dict) or not isinstance(udt_def, dict):
                logging.error("Invalid UDT data or definition types")
                return None, 0
            
            if not self._validate_udt_definition(udt_def):
                logging.error("Invalid UDT definition for encoding")
                return None, 0
            
            buffer_size = udt_def.get('size', 0)
            if buffer_size <= 0 or buffer_size > self.config.max_udt_size:
                logging.error(f"Invalid UDT size: {buffer_size}")
                return None, 0
            
            # Create and initialize buffer
            buffer = bytearray(buffer_size)
            
            for member in udt_def.get('members', []):
                try:
                    name = member.get('name', '')
                    if name not in udt_data:
                        continue  # Skip missing members
                    
                    data_type = member.get('data_type', 0)
                    offset = member.get('offset', 0)
                    array_size = member.get('array_size', 1)
                    bit_position = member.get('bit_position')
                    value = udt_data[name]
                    
                    # Validate offset
                    if offset < 0 or offset >= buffer_size:
                        logging.warning(f"UDT member {name} offset {offset} out of bounds")
                        continue
                    
                    # Encode based on array size
                    if array_size == 1:
                        # Scalar member
                        encoded = self._encode_udt_member_safe(value, data_type, bit_position)
                        if encoded and offset + len(encoded) <= buffer_size:
                            buffer[offset:offset+len(encoded)] = encoded
                    else:
                        # Array member
                        if isinstance(value, (list, tuple)):
                            element_size = EIPDataType.get_size(data_type)
                            for i, element_value in enumerate(value[:array_size]):
                                element_offset = offset + (i * element_size)
                                if element_offset + element_size <= buffer_size:
                                    encoded = self._encode_udt_member_safe(element_value, data_type)
                                    if encoded:
                                        buffer[element_offset:element_offset+len(encoded)] = encoded
                                else:
                                    break
                        
                except Exception as e:
                    logging.error(f"Failed to encode UDT member {member.get('name', 'unknown')}: {e}")
                    continue
            
            return ctypes.c_char_p(bytes(buffer)), len(buffer)
            
        except Exception as e:
            logging.error(f"Critical error encoding UDT data: {e}")
            return None, 0
    
    def _encode_udt_member_safe(self, value: Any, data_type: int, bit_position: Optional[int] = None) -> Optional[bytes]:
        """Encode individual UDT member value safely"""
        try:
            if data_type == EIPDataType.BOOL:
                if bit_position is not None:
                    # TODO: Handle packed BOOL in DWORD (requires read-modify-write)
                    return struct.pack('<I', 1 if value else 0)
                else:
                    return struct.pack('<B', 1 if value else 0)
            elif data_type == EIPDataType.SINT:
                return struct.pack('<b', max(-128, min(127, int(value))))
            elif data_type == EIPDataType.USINT:
                return struct.pack('<B', max(0, min(255, int(value))))
            elif data_type == EIPDataType.INT:
                return struct.pack('<h', max(-32768, min(32767, int(value))))
            elif data_type == EIPDataType.UINT:
                return struct.pack('<H', max(0, min(65535, int(value))))
            elif data_type == EIPDataType.DINT:
                return struct.pack('<i', max(-2147483648, min(2147483647, int(value))))
            elif data_type == EIPDataType.UDINT:
                return struct.pack('<I', max(0, min(4294967295, int(value))))
            elif data_type == EIPDataType.LINT:
                return struct.pack('<q', int(value))
            elif data_type == EIPDataType.ULINT:
                return struct.pack('<Q', max(0, int(value)))
            elif data_type == EIPDataType.REAL:
                return struct.pack('<f', float(value))
            elif data_type == EIPDataType.LREAL:
                return struct.pack('<d', float(value))
            elif data_type == EIPDataType.STRING:
                s = str(value).encode('ascii', errors='replace')
                if len(s) > self.config.max_string_length:
                    s = s[:self.config.max_string_length]
                return struct.pack('<H', len(s)) + s
            
            logging.warning(f"Unsupported UDT member data type for encoding: {data_type}")
            return None
            
        except Exception as e:
            logging.error(f"Error encoding UDT member value {value}: {e}")
            return None
    
    def _validate_udt_definition(self, udt_def: Dict[str, Any]) -> bool:
        """Validate UDT definition structure"""
        try:
            if not isinstance(udt_def, dict):
                return False
            
            required_fields = ['name', 'size', 'member_count', 'members']
            if not all(field in udt_def for field in required_fields):
                logging.error("UDT definition missing required fields")
                return False
            
            if not isinstance(udt_def['members'], list):
                logging.error("UDT members must be a list")
                return False
            
            if udt_def['size'] <= 0 or udt_def['size'] > self.config.max_udt_size:
                logging.error(f"Invalid UDT size: {udt_def['size']}")
                return False
            
            if len(udt_def['members']) != udt_def['member_count']:
                logging.warning(f"UDT member count mismatch: {len(udt_def['members'])} != {udt_def['member_count']}")
            
            # Validate each member
            for i, member in enumerate(udt_def['members']):
                if not isinstance(member, dict):
                    logging.error(f"UDT member {i} is not a dictionary")
                    return False
                
                required_member_fields = ['name', 'data_type', 'offset', 'array_size']
                if not all(field in member for field in required_member_fields):
                    logging.error(f"UDT member {i} missing required fields")
                    return False
                
                # Validate member bounds
                if member['offset'] < 0 or member['offset'] >= udt_def['size']:
                    logging.error(f"UDT member {member['name']} offset out of bounds")
                    return False
                
                if member['array_size'] <= 0 or member['array_size'] > self.config.max_array_elements:
                    logging.error(f"UDT member {member['name']} invalid array size")
                    return False
            
            return True
            
        except Exception as e:
            logging.error(f"Exception validating UDT definition: {e}")
            return False
    
    def _update_stats(self, response: EIPResponse):
        """Update worker statistics"""
        self.stats['requests_processed'] += 1
        if response.success:
            self.stats['successful_operations'] += 1
        else:
            self.stats['failed_operations'] += 1
        
        if response.response_time_us > 0:
            self.stats['total_response_time_us'] += response.response_time_us
    
    def _periodic_maintenance(self):
        """Periodic maintenance tasks"""
        current_time = time.time()
        
        # Send heartbeats every 30 seconds
        if not hasattr(self, '_last_maintenance'):
            self._last_maintenance = current_time
        
        if current_time - self._last_maintenance >= self.config.heartbeat_interval_sec:
            self._send_all_heartbeats()
            self._cleanup_dead_sessions()
            self._last_maintenance = current_time
    
    def _send_all_heartbeats(self):
        """Send heartbeats to all connected sessions"""
        for session_key, session in list(self.sessions.items()):
            try:
                result = self.eip.lib.eip_send_heartbeat(session)
                if result == EIPStatus.SUCCESS:
                    self.last_heartbeat_time[session_key] = time.time()
                    self.stats['heartbeats_sent'] += 1
                else:
                    logging.warning(f"Heartbeat failed for {session_key}: {result}")
            except Exception as e:
                logging.error(f"Exception sending heartbeat to {session_key}: {e}")
    
    def _cleanup_dead_sessions(self):
        """Clean up sessions that haven't responded to heartbeats"""
        current_time = time.time()
        dead_sessions = []
        
        for session_key, last_heartbeat in self.last_heartbeat_time.items():
            if current_time - last_heartbeat > self.config.heartbeat_interval_sec * 3:
                logging.warning(f"Session {session_key} appears dead, cleaning up")
                dead_sessions.append(session_key)
        
        for session_key in dead_sessions:
            self._cleanup_session(session_key)
    
    def _cleanup(self):
        """Cleanup all resources"""
        logging.info(f"Worker {self.worker_id} starting cleanup")
        
        # Close all sessions
        for session_key in list(self.sessions.keys()):
            self._cleanup_session(session_key)
        
        # Clear caches
        self.udt_definitions.clear()
        self.session_metadata.clear()
        self.last_heartbeat_time.clear()
        
        # Log final statistics
        uptime = time.time() - self.stats['start_time']
        total_requests = self.stats['requests_processed']
        success_rate = (self.stats['successful_operations'] / max(total_requests, 1)) * 100
        
        if total_requests > 0:
            avg_response_time = self.stats['total_response_time_us'] / total_requests / 1000  # ms
            requests_per_second = total_requests / max(uptime, 1)
            
            logging.info(f"Worker {self.worker_id} final stats:")
            logging.info(f"  Uptime: {uptime:.1f}s")
            logging.info(f"  Requests: {total_requests}")
            logging.info(f"  Success rate: {success_rate:.1f}%")
            logging.info(f"  Avg response time: {avg_response_time:.2f}ms")
            logging.info(f"  Requests/sec: {requests_per_second:.1f}")
            logging.info(f"  Reconnects: {self.stats['successful_reconnects']}/{self.stats['reconnect_attempts']}")
            logging.info(f"  UDTs cached: {self.stats['udt_definitions_cached']}")
            logging.info(f"  Heartbeats sent: {self.stats['heartbeats_sent']}")

# =============================================================================
# Industrial-Grade ZMQ Manager
# =============================================================================

class IndustrialZMQEIPManager:
    """Production-grade ZMQ + libeip manager with comprehensive error handling"""
    
    def __init__(self, config: EIPConfig = None, ventilator_port: int = 5557, 
                 sink_port: int = 5558, libeip_path: str = "./libeip.so"):
        self.config = config or EIPConfig()
        self.config.validate()
        
        self.ventilator_port = ventilator_port
        self.sink_port = sink_port
        self.libeip_path = libeip_path
        
        # Process management
        self.workers = []
        self.context = None
        self.ventilator = None
        self.sink = None
        
        # Request management
        self.pending_requests = {}
        self.request_counter = 0
        self.running = False
        
        # Background threads
        self.collector_thread = None
        self.monitor_thread = None
        
        # System monitoring
        self.system_stats = {
            'start_time': 0,
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'total_response_time_us': 0,
            'active_workers': 0,
            'queue_depth': 0
        }
        
        # Shutdown handling
        self.shutdown_event = threading.Event()
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logging.info(f"Manager received signal {signum}, initiating shutdown...")
        self.shutdown()
    
    def start(self):
        """Start the industrial-grade EIP system"""
        if self.running:
            logging.warning("System already running")
            return
        
        logging.info(f"Starting Industrial ZMQ EIP Manager")
        logging.info(f"Configuration: {self.config}")
        
        try:
            # Setup ZMQ context with proper settings
            self.context = zmq.Context()
            self.context.setsockopt(zmq.MAX_SOCKETS, 1024)
            
            # Setup ventilator (distributes work to workers)
            self.ventilator = self.context.socket(zmq.PUSH)
            self.ventilator.setsockopt(zmq.LINGER, self.config.zmq_linger_ms)
            self.ventilator.setsockopt(zmq.SNDHWM, self.config.zmq_high_water_mark)
            self.ventilator.bind(f"tcp://*:{self.ventilator_port}")
            
            # Setup sink (collects results from workers)
            self.sink = self.context.socket(zmq.PULL)
            self.sink.setsockopt(zmq.LINGER, self.config.zmq_linger_ms)
            self.sink.setsockopt(zmq.RCVHWM, self.config.zmq_high_water_mark)
            self.sink.bind(f"tcp://*:{self.sink_port}")
            
            # Start workers
            self._start_workers()
            
            # Start background threads
            self.running = True
            self.system_stats['start_time'] = time.time()
            
            self.collector_thread = threading.Thread(target=self._collect_responses, daemon=True)
            self.collector_thread.start()
            
            self.monitor_thread = threading.Thread(target=self._system_monitor, daemon=True)
            self.monitor_thread.start()
            
            # Wait for workers to initialize
            time.sleep(2.0)
            
            logging.info(f"Industrial EIP Manager started successfully with {len(self.workers)} workers")
            
        except Exception as e:
            logging.error(f"Failed to start system: {e}")
            self.shutdown()
            raise
    
    def _start_workers(self):
        """Start worker processes"""
        for i in range(self.config.worker_count):
            try:
                worker = IndustrialEIPWorker(
                    worker_id=i,
                    ventilator_port=self.ventilator_port,
                    sink_port=self.sink_port,
                    config=self.config,
                    libeip_path=self.libeip_path
                )
                worker.start()
                self.workers.append(worker)
                logging.info(f"Started worker {i}")
                
            except Exception as e:
                logging.error(f"Failed to start worker {i}: {e}")
                # Clean up any started workers
                for worker in self.workers:
                    worker.terminate()
                raise
        
        self.system_stats['active_workers'] = len(self.workers)
    
    def shutdown(self):
        """Graceful shutdown of the entire system"""
        if not self.running:
            return
        
        logging.info("Starting system shutdown...")
        self.running = False
        self.shutdown_event.set()
        
        try:
            # Stop background threads
            if self.collector_thread and self.collector_thread.is_alive():
                self.collector_thread.join(timeout=5)
            
            if self.monitor_thread and self.monitor_thread.is_alive():
                self.monitor_thread.join(timeout=5)
            
            # Stop workers gracefully
            logging.info("Stopping workers...")
            for worker in self.workers:
                if worker.is_alive():
                    worker.terminate()
                    worker.join(timeout=5)
                    if worker.is_alive():
                        logging.warning(f"Worker {worker.worker_id} did not stop gracefully")
            
            # Close ZMQ sockets
            if self.ventilator:
                self.ventilator.close()
            if self.sink:
                self.sink.close()
            
            # Terminate ZMQ context
            if self.context:
                self.context.term()
            
            # Clear pending requests
            self.pending_requests.clear()
            
            # Log final statistics
            self._log_final_stats()
            
            logging.info("System shutdown complete")
            
        except Exception as e:
            logging.error(f"Error during shutdown: {e}")
    
    def _log_final_stats(self):
        """Log final system statistics"""
        uptime = time.time() - self.system_stats['start_time']
        total_requests = self.system_stats['total_requests']
        success_rate = (self.system_stats['successful_requests'])