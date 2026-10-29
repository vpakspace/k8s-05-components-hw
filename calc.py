#!/usr/bin/env python3
"""Расчёт кластера Kubernetes под проект (ДЗ «Компоненты Kubernetes»).

Запуск: python3 calc.py
Единицы: CPU — ядра (vCPU), память — ГБ. 1 ГБ = 1000 МБ (как в условии задачи);
узлы считаются по номиналу (16 ГБ), то есть с небольшим консервативным запасом.
"""
from dataclasses import dataclass

# --- допущения ---------------------------------------------------------------
# DaemonSet'ы на каждой ноде (CNI, kube-proxy, агент логов, node-exporter) — фиксированная цена ноды.
DS_CPU, DS_MEM = 0.3, 0.5
EVICTION_MEM = 0.1  # порог вытеснения kubelet, ГБ


@dataclass(frozen=True)
class Component:
    name: str
    replicas: int
    cpu: float  # на одну реплику
    mem: float  # ГБ на одну реплику
    spread: bool = False  # реплики обязаны лежать на разных нодах (отказоустойчивость)


COMPONENTS = [
    Component("База данных", 3, 1.0, 4.0, spread=True),
    Component("Кеш", 3, 1.0, 4.0, spread=True),
    Component("Фронтенд", 5, 0.2, 0.05),
    Component("Бекенд", 10, 1.0, 0.6),
]


def reserved_cpu(cores: float) -> float:
    """Резерв под kubelet и ОС: 6% 1-го ядра, 1% 2-го, 0.5% ядер 3–4, 0.25% остальных."""
    return (0.06 * min(cores, 1) + 0.01 * min(max(cores - 1, 0), 1)
            + 0.005 * min(max(cores - 2, 0), 2) + 0.0025 * max(cores - 4, 0))


def reserved_mem(gb: float, slip: float = 0.0) -> float:
    """Резерв памяти: 25% первых 4 ГБ, 20% следующих 4, 10% следующих 8, 6% следующих 112, 2% остальных.
    slip — поправка на «2.8 вместо 2.6 ГБ» из разбора узла на 16 ГБ в статье learnk8s (см. README)."""
    tiers = [(0.25, 4), (0.20, 4), (0.10, 8), (0.06, 112)]
    total, left = 0.0, gb
    for rate, size in tiers:
        part = min(left, size)
        total += rate * part
        left -= part
    total += 0.02 * max(left, 0)
    return total + EVICTION_MEM + slip


def allocatable(cores: float, gb: float, slip: float = 0.0) -> tuple[float, float]:
    """Что остаётся Pod'ам: номинал − резерв kubelet/ОС − eviction − DaemonSet'ы."""
    return cores - reserved_cpu(cores) - DS_CPU, gb - reserved_mem(gb, slip) - DS_MEM


def pods() -> list[tuple[str, float, float, bool]]:
    return [(f"{c.name}-{i + 1}", c.cpu, c.mem, c.spread)
            for c in COMPONENTS for i in range(c.replicas)]


def place(items, nodes: dict[str, dict]) -> bool:
    """Жадное размещение «как у планировщика»: самая свободная нода, реплики БД/кеша — по одной на ноду."""
    for name, cpu, mem, spread in sorted(items, key=lambda p: (-p[2], -p[1])):
        kind = name.rsplit("-", 1)[0]
        fits = [n for n, s in nodes.items()
                if s["cpu_free"] >= cpu and s["mem_free"] >= mem and not (spread and kind in s["kinds"])]
        if not fits:
            return False
        # как LeastAllocated в kube-scheduler: побеждает нода с наибольшей средней долей свободных CPU и памяти
        best = max(fits, key=lambda n: (nodes[n]["cpu_free"] / nodes[n]["cpu"] + nodes[n]["mem_free"] / nodes[n]["mem"]) / 2)
        s = nodes[best]
        s["cpu_free"] -= cpu
        s["mem_free"] -= mem
        s["pods"].append((name, cpu, mem, spread))
        if spread:
            s["kinds"].add(kind)
    return True


def fresh_nodes(n: int, cores: float, gb: float, slip: float = 0.0) -> dict[str, dict]:
    a_cpu, a_mem = allocatable(cores, gb, slip)
    return {f"node{i + 1}": dict(cpu=a_cpu, mem=a_mem, cpu_free=a_cpu, mem_free=a_mem, pods=[], kinds=set())
            for i in range(n)}


def after_failure(nodes: dict[str, dict], dead: str) -> tuple[bool, float, float]:
    """Роняем ноду dead и пересаживаем её Pod'ы на остальные.
    Возвращает (всё разместилось, макс. загрузка CPU, макс. загрузка RAM среди оставшихся)."""
    rest = {k: dict(v, pods=list(v["pods"]), kinds=set(v["kinds"])) for k, v in nodes.items() if k != dead}
    if not place(nodes[dead]["pods"], rest):
        return False, 0.0, 0.0
    return (True,
            max(1 - s["cpu_free"] / s["cpu"] for s in rest.values()),
            max(1 - s["mem_free"] / s["mem"] for s in rest.values()))


def survives_any_failure(n: int, cores: float, gb: float, slip: float = 0.0) -> tuple[bool, float, float]:
    """Размещаем всё на n нодах, затем по очереди «роняем» каждую.
    Возвращает (выжили ли при любом отказе, макс. загрузка CPU, макс. загрузка RAM на оставшихся нодах)."""
    nodes = fresh_nodes(n, cores, gb, slip)
    if not place(pods(), nodes):
        return False, 0.0, 0.0
    results = [after_failure(nodes, dead) for dead in nodes]
    if not all(ok for ok, _, _ in results):
        return False, 0.0, 0.0
    return True, max(c for _, c, _ in results), max(m for _, _, m in results)


def main() -> None:
    tot_cpu = sum(c.replicas * c.cpu for c in COMPONENTS)
    tot_mem = sum(c.replicas * c.mem for c in COMPONENTS)
    print("1. Ресурсы приложения")
    for c in COMPONENTS:
        print(f"   {c.name:<14}{c.replicas:>3} x ({c.cpu:g} CPU, {c.mem * 1000:g} МБ) = "
              f"{c.replicas * c.cpu:g} CPU, {c.replicas * c.mem:g} ГБ")
    print(f"   ИТОГО: {tot_cpu:g} CPU, {tot_mem:g} ГБ, {sum(c.replicas for c in COMPONENTS)} Pod'ов\n")

    print("2. Что остаётся Pod'ам на ноде (формула резервов + DaemonSet'ы)")
    for cores, gb in [(2, 8), (4, 16), (6, 16), (8, 16), (8, 32)]:
        a_cpu, a_mem = allocatable(cores, gb)
        print(f"   {cores} vCPU / {gb:>2} ГБ: резерв {reserved_cpu(cores):.3f} CPU и {reserved_mem(gb):.2f} ГБ "
              f"-> Pod'ам {a_cpu:.2f} CPU, {a_mem:.2f} ГБ")

    print("\n3. Варианты рабочих нод: хватит ли после отказа любой одной ноды")
    print(f"   {'вариант':<18}{'всего':<14}{'(n-1) нод: CPU / ГБ':<22}{'агрегат':<9}{'размещение':<11}макс. загрузка CPU/RAM")
    for n, cores, gb in [(3, 8, 16), (4, 4, 16), (4, 6, 16), (4, 8, 12), (4, 8, 16),
                         (5, 4, 16), (5, 6, 16), (5, 8, 16), (6, 4, 16), (6, 4, 8)]:
        a_cpu, a_mem = allocatable(cores, gb)
        agg = (n - 1) * a_cpu >= tot_cpu and (n - 1) * a_mem >= tot_mem
        ok, u_cpu, u_mem = survives_any_failure(n, cores, gb)
        print(f"   {n} x {cores} vCPU/{gb:>2} ГБ  {n * cores:>2} vCPU/{n * gb:>3} ГБ  "
              f"{(n - 1) * a_cpu:>5.1f} / {(n - 1) * a_mem:>5.1f}      {'да' if agg else 'нет':<9}"
              f"{'да' if ok else 'нет':<11}{f'{u_cpu:.0%} / {u_mem:.0%}' if ok else '-'}")

    print("\n4. Выбранные 4 x (8 vCPU, 16 ГБ): размещение в штатном режиме")
    nodes = fresh_nodes(4, 8, 16)
    place(pods(), nodes)
    for name, s in nodes.items():
        used_c, used_m = s["cpu"] - s["cpu_free"], s["mem"] - s["mem_free"]
        kinds: dict[str, int] = {}
        for p in s["pods"]:
            k = p[0].rsplit("-", 1)[0]
            kinds[k] = kinds.get(k, 0) + 1
        print(f"   {name}: {used_c:.2f}/{s['cpu']:.2f} CPU, {used_m:.2f}/{s['mem']:.2f} ГБ  {kinds}")

    print("\n5. Отказ каждой ноды по очереди: все ли Pod'ы разместились на оставшихся трёх")
    for dead in nodes:
        ok, u_cpu, u_mem = after_failure(nodes, dead)
        print(f"   отказ {dead}: {'да' if ok else 'НЕТ'}; макс. загрузка оставшихся: CPU {u_cpu:.0%}, RAM {u_mem:.0%}")
    ok2, c2, m2 = survives_any_failure(4, 8, 16, slip=0.2)
    print(f"   с «2.8 ГБ» из статьи (резерв +0.2 ГБ на ноду): {'да' if ok2 else 'НЕТ'}; худший случай CPU {c2:.0%}, RAM {m2:.0%}")


if __name__ == "__main__":
    main()
