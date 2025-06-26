import subprocess

from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

console = Console()

instructions = """[bold yellow]Bem-vindo ao Sistema de Processamento de Requisições![/bold yellow]

[green]Não esqueça dos passos abaixo:[/green]
1. [cyan]Cole a chave de autenticação no .env[/cyan]
2. [cyan]Coloque os arquivos de input no seu respectivo diretório[/cyan]
3. [cyan]Ajuste o nome do arquivo de input e output no script do processo[/cyan]
4. [cyan]Ajuste o nome da coluna que possui o código do imóvel (se necessário)[/cyan]
5. [cyan]Ajuste o nome do arquivo de output (se necessário)[/cyan]
"""

options = {
    "1": (
        "[bold cyan]🔄 Request[/bold cyan] de Upload de Geometrias",
        "python3 -m request_process.uploads.geometries_upload",
    ),
    "2": (
        "[bold cyan]🔄 Request[/bold cyan] de Desmatamento MapBiomas (batch unificado, unifica e processa as geometrias como um todo)",
        "python3 -m request_process.deforestation.batch_deforestation_mapbiomas_batch_request",
    ),
    "3": (
        "[bold cyan]🔄 Request[/bold cyan] de Desmatamento MapBiomas",
        "python3 -m request_process.deforestation.deforestation_mapbiomas_batch_request",
    ),
    "4": (
        "[bold cyan]🔄 Request[/bold cyan] de Desmatamento PRODES",
        "python3 -m request_process.deforestation.deforestation_prodes_batch_request",
    ),
    "5": (
        "[bold cyan]🔄 Request[/bold cyan] de Relatório Detalhado",
        "python3 -m request_process.detailed_report.report_detailedbatch_request",
    ),
    "6": (
        "[bold magenta]📥 Get[/bold magenta] de Resultados de Desmatamento",
        "python3 -m get_process.get_batch_deforestation_results",
    ),
    "7": (
        "[bold magenta]📥 Get[/bold magenta] de Relatórios Detalhados Gerados",
        "python3 -m get_process.get_report_detailed_batch",
    ),
    "8": (
        "[bold magenta]📥 Get[/bold magenta] de Interseção CAR × Área Restrita (relatório simples)",
        "python3 -m simple_requests.car_intersect_restricted_area",
    ),
    "9": (
        "[bold green]⬇️ Download[/bold green] de Resultados",
        "python3 -m download",
    ),
    "0": (
        "[bold red]❌ Sair[/bold red]",
        None,
    ),
}


def print_menu():
    table = Table(
        title="📊 Menu de Processos",
        box=box.ROUNDED,
        show_lines=True,
        title_style="bold green",
    )
    table.add_column("Opção", style="cyan bold", width=6, justify="center")
    table.add_column("Descrição", style="white", no_wrap=False)

    for key, (desc, _) in options.items():
        table.add_row(f"[bold]{key}[/bold]", desc)

    console.print(table)


def main():
    console.print(
        Panel(
            instructions,
            title="⚙️ Instruções",
            title_align="left",
            border_style="bright_yellow",
        )
    )

    while True:
        print_menu()
        choice = console.input(
            "\n[bold cyan]Digite o número da opção[/bold cyan]: "
        ).strip()

        if choice not in options:
            console.print("[bold red]❌ Opção inválida. Tente novamente.[/bold red]")
            continue

        if choice == "0":
            console.print("[bold green]✅ Encerrado com sucesso.[/bold green]")
            break

        _, command = options[choice]
        console.print(
            f"\n[bold green]▶️ Executando:[/bold green] [yellow]{command}[/yellow]\n"
        )
        try:
            subprocess.run(command, shell=True, check=True)
        except subprocess.CalledProcessError as e:
            console.print(f"[red]❌ Erro ao executar o comando:[/red] {e}")


if __name__ == "__main__":
    main()
