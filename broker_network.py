
import socket
import threading
import struct
import time
from queue import Queue, Empty


class NodeConnection:
    """
    Representa la conexión TCP activa con un nodo trabajador (Worker).
    Maneja el protocolo binario para enviar y recibir fotogramas:
    Protocolo de red:
      [frame_id: 4 bytes unsigned int] + [data_len: 4 bytes unsigned int] + [payload: data_len bytes]
    """
    def __init__(self, node_id: int, sock: socket.socket, address: tuple):
        self.node_id = node_id
        self.sock = sock
        self.address = address
        self.is_active = True

    def send_frame(self, frame_id: int, frame_bytes: bytes):
        """Envía un fotograma al worker empaquetando ID y longitud."""
        header = struct.pack("!II", frame_id, len(frame_bytes))
        self.sock.sendall(header + frame_bytes)

    def receive_frame(self) -> tuple[int, bytes]:
        """
        Recibe el fotograma procesado devuelto por el worker.
        Retorna (frame_id, frame_bytes) o (None, None) si la conexión se cierra.
        """
        header = self._recv_exact(8)
        if not header:
            return None, None
        frame_id, length = struct.unpack("!II", header)
        data = self._recv_exact(length)
        if data is None:
            return None, None
        return frame_id, data

    def _recv_exact(self, num_bytes: int) -> bytes:
        """Lee exactamente num_bytes del socket TCP de forma bloqueante."""
        buf = bytearray()
        while len(buf) < num_bytes:
            packet = self.sock.recv(num_bytes - len(buf))
            if not packet:
                return None
            buf.extend(packet)
        return bytes(buf)

    def close(self):
        """Cierra el socket y marca el nodo como inactivo."""
        self.is_active = False
        try:
            self.sock.close()
        except Exception:
            pass


class BrokerNodeManager:
    """
    Servidor de gestión de red del Broker:
    - Escucha y registra nodos de trabajo.
    - Asegura la existencia de un quórum mínimo (por defecto 3).
    - Despacha fotogramas en paralelo a través de un pool de hilos dedicados.
    """
    def __init__(self, host: str = "0.0.0.0", port: int = 6000, min_workers: int = 3):
        self.host = host
        self.port = port
        self.min_workers = min_workers

        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        self.workers: dict[int, NodeConnection] = {}
        self.lock = threading.Lock()
        self.node_counter = 0

        self.task_queue: Queue = Queue()
        self.results: dict[int, bytes] = {}
        self.running = False

    def start_server(self):
        """Inicia el socket servidor y el hilo en segundo plano para escuchar workers."""
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(10)
        self.running = True
        print(f"[BROKER-RED] Servidor escuchando registros de workers en {self.host}:{self.port}")

        listener_thread = threading.Thread(target=self._accept_loop, daemon=True)
        listener_thread.start()

    def _accept_loop(self):
        """Hilo para aceptar conexiones entrantes y registrar nuevos nodos."""
        while self.running:
            try:
                client_sock, addr = self.server_socket.accept()
                with self.lock:
                    self.node_counter += 1
                    node_id = self.node_counter
                    node = NodeConnection(node_id, client_sock, addr)
                    self.workers[node_id] = node

                print(f"[BROKER-RED] Nodo #{node_id} registrado exitosamente desde {addr}. "
                      f"Total conectados: {len(self.workers)}")
            except Exception as e:
                if self.running:
                    print(f"[BROKER-RED] Error al aceptar conexión: {e}")
                break

    def get_active_workers_count(self) -> int:
        """Devuelve la cantidad actual de nodos conectados."""
        with self.lock:
            return len(self.workers)

    def wait_for_min_workers(self, timeout: float = None) -> bool:
        """
        Bloquea la ejecución hasta que se conecte el mínimo de nodos requerido (3 por defecto).
        """
        start = time.time()
        print(f"[BROKER-RED] Verificando quórum: se requieren al menos {self.min_workers} nodos activos...")
        while self.get_active_workers_count() < self.min_workers:
            if timeout and (time.time() - start) > timeout:
                print(f"[BROKER-RED] Tiempo de espera agotado ({timeout}s).")
                return False
            time.sleep(1)

        print(f"[BROKER-RED] Quórum listo: {self.get_active_workers_count()} nodos activos.")
        return True

    def _worker_dispatcher_loop(self, node: NodeConnection):
        """
        Hilo dedicado para un nodo específico. Toma fotogramas de la cola común,
        se los envía al worker, espera la respuesta procesada y la almacena.
        """
        while self.running and node.is_active:
            try:
                # Extrae un fotograma pendiente de la cola
                frame_id, frame_data = self.task_queue.get(timeout=1.0)
            except Empty:
                # No hay tareas en este momento, sigue a la espera
                continue

            try:
                # 1. Enviar fotograma al nodo
                node.send_frame(frame_id, frame_data)

                # 2. Recibir fotograma procesado
                ret_id, processed_data = node.receive_frame()
                if ret_id is None:
                    raise ConnectionResetError(f"Nodo #{node.node_id} cerró la conexión inesperadamente.")

                # 3. Registrar resultado de forma thread-safe
                with self.lock:
                    self.results[ret_id] = processed_data

                # Marcar tarea como completada en la cola
                self.task_queue.task_done()
                print(f"[BROKER-RED] Frame #{ret_id} procesado exitosamente por Nodo #{node.node_id}")

            except Exception as e:
                print(f"[BROKER-RED] Fallo de comunicación con Nodo #{node.node_id}: {e}")
                # Tolerancia a fallos: re-encolar el fotograma para que otro nodo lo procese
                self.task_queue.put((frame_id, frame_data))
                self.task_queue.task_done()

                # Desconectar y remover nodo con falla
                with self.lock:
                    node.close()
                    self.workers.pop(node.node_id, None)
                print(f"[BROKER-RED] Nodo #{node.node_id} eliminado. Nodos activos restantes: {len(self.workers)}")
                break

    def process_frames(self, frames: list[tuple[int, bytes]]) -> dict[int, bytes]:
        """
        Interfaz principal para Parte 3 (División y Unión de video):
        Recibe una lista de tuplas [(frame_id, bytes_imagen), ...]
        Reparte el trabajo en paralelo entre los nodos usando hilos concurrentes.
        Retorna un diccionario {frame_id: bytes_procesados} ordenable por Parte 3.
        """
        if not self.wait_for_min_workers():
            raise RuntimeError(f"Error: No se alcanzó el mínimo de {self.min_workers} nodos.")

        self.results.clear()

        # 1. Cargar todos los fotogramas en la cola de tareas compartida
        for frame in frames:
            self.task_queue.put(frame)

        print(f"[BROKER-RED] Se encolaron {len(frames)} fotogramas para repartir.")

        # 2. Lanzar hilos despachadores para cada nodo activo
        threads = []
        with self.lock:
            active_nodes = list(self.workers.values())

        for node in active_nodes:
            t = threading.Thread(target=self._worker_dispatcher_loop, args=(node,), daemon=True)
            threads.append(t)
            t.start()

        # 3. Esperar hasta que todos los fotogramas en cola sean procesados
        self.task_queue.join()
        print(f"[BROKER-RED] Todos los fotogramas han sido procesados ({len(self.results)} total).")
        return dict(self.results)

    def stop(self):
        """Detiene el servidor y cierra todas las conexiones de nodos."""
        self.running = False
        with self.lock:
            for node in self.workers.values():
                node.close()
            self.workers.clear()
        try:
            self.server_socket.close()
        except Exception:
            pass
        print("[BROKER-RED] Servidor de nodos detenido.")


if __name__ == "__main__":
    # Prueba rápida del componente
    print("--- Iniciando Broker (Parte 2: Gestión de Nodos y Reparto) ---")
    broker = BrokerNodeManager(host="0.0.0.0", port=6000, min_workers=3)
    broker.start_server()

    try:
        # Espera que los workers se conecten (mínimo 3)
        broker.wait_for_min_workers()
        print("Servidor listo para recibir solicitudes de procesamiento.")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nDeteniendo servidor...")
        broker.stop()

